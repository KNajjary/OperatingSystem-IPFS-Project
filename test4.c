// Build: gcc -O2 -pthread -o c_engine c_engine.c -lblake3
// Run:   ./c_engine /tmp/cengine.sock

#define _GNU_SOURCE
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <blake3.h>
#include <time.h>

// ==================== تعریف ثابت‌ها ====================
#define OP_UPLOAD_START    0x01
#define OP_UPLOAD_CHUNK    0x02
#define OP_UPLOAD_FINISH   0x03
#define OP_UPLOAD_DONE     0x81

#define OP_DOWNLOAD_START  0x10  // اصلاح شده: باید 0x10 باشد نه 0x11
#define OP_DOWNLOAD_CHUNK  0x90  // اصلاح شده: باید 0x90 باشد نه 0x91
#define OP_DOWNLOAD_DONE   0x91  // اصلاح شده: باید 0x91 باشد نه 0x92

#define OP_ERROR           0xFF

#define CHUNK_SIZE        (256 * 1024)  // 256KB - اصلاح تایپ: CHUNCK_BITS -> CHUNK_SIZE
#define HASH_LENGTH       32
#define MAX_THREADS       8
#define MAX_TASKS         100
#define CID_PREFIX        "b"
#define HASH_ALGO         "blake3"

// کدهای خطا
#define E_BAD_CID         "E_BAD_CID"
#define E_NOT_FOUND       "E_NOT_FOUND"
#define E_HASH_MISMATCH   "E_HASH_MISMATCH"
#define E_BUSY            "E_BUSY"
#define E_PROTO           "E_PROTO"

// ==================== ساختارهای داده ====================

typedef struct {
    char *filename;
    long long total_size;
    int chunk_count;
    int chunks_received;
    pthread_mutex_t lock;
    int client_fd;
} upload_state_t;

typedef struct {
    char cid[128];
    int client_fd;
} download_state_t;

// ==================== متغیرهای سراسری ====================
static const char *g_sock_path = NULL;
static const char *BLOCKS_DIR = "blocks";
static const char *MANIFESTS_DIR = "manifests";

// ==================== توابع کمکی ====================

// ایجاد پوشه
void ensure_dir(const char *path) {
    struct stat st;
    if (stat(path, &st) == -1) {
        mkdir(path, 0755);
    }
}

// خواندن دقیق n بایت
ssize_t read_n(int fd, void *buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, (char*)buf + got, n - got);
        if (r == 0) return 0;
        if (r < 0) { 
            if (errno == EINTR) continue; 
            return -1; 
        }
        got += r;
    }
    return (ssize_t)got;
}

// نوشتن کامل n بایت
int write_all(int fd, const void *buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        ssize_t w = write(fd, (const char*)buf + sent, n - sent);
        if (w < 0) { 
            if (errno == EINTR) continue; 
            return -1; 
        }
        sent += (size_t)w;
    }
    return 0;
}

// ارسال فریم
int send_frame(int fd, uint8_t op, const void* payload, uint32_t len) {
    uint8_t header[5];
    header[0] = op;
    uint32_t be_len = htonl(len);
    memcpy(header + 1, &be_len, 4);
    if (write_all(fd, header, 5) < 0) return -1;
    if (len && write_all(fd, payload, len) < 0) return -1;
    return 0;
}

// ارسال خطا
int send_error(int fd, const char *code, const char *message) {
    char error_json[256];
    snprintf(error_json, sizeof(error_json),
             "{\"code\":\"%s\",\"message\":\"%s\"}", code, message);
    return send_frame(fd, OP_ERROR, error_json, strlen(error_json));
}

// ==================== توابع آپلود (ساده شده) ====================

// پردازش شروع آپلود
upload_state_t* handle_upload_start(int client_fd, const uint8_t *payload, uint32_t len) {
    printf("[ENGINE] UPLOAD_START received: %.*s\n", (int)len, payload);
    
    // پارس کردن payload (فرمت: filename|total_size|chunk_size|hash_algo)
    char *payload_str = malloc(len + 1);
    memcpy(payload_str, payload, len);
    payload_str[len] = '\0';
    
    char *token = strtok(payload_str, "|");
    if (!token) {
        free(payload_str);
        send_error(client_fd, E_PROTO, "Invalid upload start format");
        return NULL;
    }
    
    upload_state_t *state = malloc(sizeof(upload_state_t));
    if (!state) {
        free(payload_str);
        send_error(client_fd, E_BUSY, "System busy");
        return NULL;
    }
    
    state->filename = strdup(token);
    
    token = strtok(NULL, "|");
    if (token) state->total_size = atoll(token);
    
    // محاسبه تعداد چانک‌ها
    state->chunk_count = (state->total_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
    state->chunks_received = 0;
    state->client_fd = client_fd;
    
    pthread_mutex_init(&state->lock, NULL);
    
    free(payload_str);
    
    printf("[ENGINE] Upload started: %s, size: %lld, chunks: %d\n",
           state->filename, state->total_size, state->chunk_count);
    
    return state;
}

// پردازش چانک آپلود
void handle_upload_chunk(int client_fd, const uint8_t *payload, uint32_t len,
                         upload_state_t *state) {
    if (!state) {
        send_error(client_fd, E_PROTO, "No upload started");
        return;
    }
    
    printf("[ENGINE] UPLOAD_CHUNK received, size: %u\n", len);
    
    pthread_mutex_lock(&state->lock);
    state->chunks_received++;
    pthread_mutex_unlock(&state->lock);
    
    // در این نسخه ساده، فقط لاگ می‌کنیم
    // در نسخه کامل باید داده ذخیره و هش محاسبه شود
}

// پردازش پایان آپلود
void handle_upload_finish(int client_fd, upload_state_t *state) {
    if (!state) {
        send_error(client_fd, E_PROTO, "No upload started");
        return;
    }
    
    printf("[ENGINE] UPLOAD_FINISH received\n");
    
    // ایجاد یک CID نمونه (در نسخه واقعی باید بر اساس هش مانیفست باشد)
    char *cid = "bciq2kn5d6m7e8f9g0h1i2j3k4l5m6n7o8p9q0r1s2t3u4v5w6x7y8z";
    
    // ارسال پاسخ
    send_frame(client_fd, OP_UPLOAD_DONE, cid, strlen(cid));
    
    printf("[ENGINE] Upload completed: CID=%s\n", cid);
}

// ==================== توابع دانلود (ساده شده) ====================

// شروع دانلود
download_state_t* download_start(int client_fd, const char *cid) {
    printf("[ENGINE] DOWNLOAD_START received: cid=%s\n", cid);
    
    download_state_t *state = malloc(sizeof(download_state_t));
    if (!state) {
        send_error(client_fd, E_BUSY, "System busy");
        return NULL;
    }
    
    strncpy(state->cid, cid, sizeof(state->cid) - 1);
    state->cid[sizeof(state->cid) - 1] = '\0';
    state->client_fd = client_fd;
    
    printf("[ENGINE] Download started for CID: %s\n", cid);
    
    return state;
}

// دانلود چانک‌ها
void download_chunks(download_state_t *state) {
    if (!state) return;
    
    printf("[ENGINE] Simulating download of chunks for CID: %s\n", state->cid);
    
    // در این نسخه ساده، یک چانک نمونه می‌فرستیم
    char *test_data = "This is a test chunk from the engine";
    uint32_t index = 0;
    uint32_t size = strlen(test_data);
    
    // ساخت payload: [index:4byte][size:4byte][data:variable]
    uint8_t *payload = malloc(8 + size);
    uint32_t network_index = htonl(index);
    uint32_t network_size = htonl(size);
    
    memcpy(payload, &network_index, 4);
    memcpy(payload + 4, &network_size, 4);
    memcpy(payload + 8, test_data, size);
    
    send_frame(state->client_fd, OP_DOWNLOAD_CHUNK, payload, 8 + size);
    
    free(payload);
    printf("[ENGINE] Sent one sample chunk\n");
}

// پایان دانلود
void download_done(download_state_t *state) {
    if (!state) return;
    
    printf("[ENGINE] Sending DOWNLOAD_DONE for CID: %s\n", state->cid);
    send_frame(state->client_fd, OP_DOWNLOAD_DONE, NULL, 0);
    free(state);
}

// ==================== پردازش اتصال ====================

void handle_connection(int client_fd) {
    upload_state_t *upload_state = NULL;
    download_state_t *download_state = NULL;
    
    printf("[ENGINE] New connection from client\n");
    
    while (1) {
        uint8_t header[5];
        if (read_n(client_fd, header, 5) <= 0) break;
        
        uint8_t opcode = header[0];
        uint32_t length;
        memcpy(&length, header + 1, 4);
        length = ntohl(length);
        
        uint8_t *payload = NULL;
        if (length > 0) {
            payload = malloc(length);
            if (!payload) {
                send_error(client_fd, E_BUSY, "Out of memory");
                break;
            }
            if (read_n(client_fd, payload, length) <= 0) {
                free(payload);
                break;
            }
        }
        
        printf("[ENGINE] Received opcode: 0x%02x, length: %u\n", opcode, length);
        
        switch (opcode) {
            case OP_UPLOAD_START:
                if (upload_state) {
                    send_error(client_fd, E_PROTO, "Upload already in progress");
                } else {
                    upload_state = handle_upload_start(client_fd, payload, length);
                }
                break;
                
            case OP_UPLOAD_CHUNK:
                handle_upload_chunk(client_fd, payload, length, upload_state);
                break;
                
            case OP_UPLOAD_FINISH:
                handle_upload_finish(client_fd, upload_state);
                // پاکسازی upload_state
                if (upload_state) {
                    free(upload_state->filename);
                    pthread_mutex_destroy(&upload_state->lock);
                    free(upload_state);
                    upload_state = NULL;
                }
                break;
                
            case OP_DOWNLOAD_START: {
                if (!payload || length == 0) {
                    send_error(client_fd, E_PROTO, "No CID provided");
                    break;
                }
                
                char *cid = malloc(length + 1);
                memcpy(cid, payload, length);
                cid[length] = '\0';
                
                download_state = download_start(client_fd, cid);
                free(cid);
                
                if (download_state) {
                    download_chunks(download_state);
                    download_done(download_state);
                    download_state = NULL;
                }
                break;
            }
                
            default:
                printf("[ENGINE] Unknown opcode: 0x%02x\n", opcode);
                send_error(client_fd, E_PROTO, "Invalid opcode");
                break;
        }
        
        if (payload) free(payload);
    }
    
    // پاکسازی در صورت خروج غیرعادی
    if (upload_state) {
        free(upload_state->filename);
        pthread_mutex_destroy(&upload_state->lock);
        free(upload_state);
    }
    
    close(client_fd);
    printf("[ENGINE] Connection closed\n");
}

// ==================== تابع اصلی ====================

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <socket_path>\n", argv[0]);
        fprintf(stderr, "Example: %s /tmp/cengine.sock\n", argv[0]);
        return 1;
    }
    
    g_sock_path = argv[1];
    
    // ایجاد پوشه‌ها
    ensure_dir(BLOCKS_DIR);
    ensure_dir(MANIFESTS_DIR);
    
    // ایجاد سوکت
    int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return 1;
    }
    
    // تنظیم SO_REUSEADDR برای جلوگیری از خطای "Address already in use"
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, g_sock_path, sizeof(addr.sun_path) - 1);
    
    // حذف فایل سوکت قبلی
    unlink(g_sock_path);
    
    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(server_fd);
        return 1;
    }
    
    if (listen(server_fd, 64) < 0) {
        perror("listen");
        close(server_fd);
        unlink(g_sock_path);
        return 1;
    }
    
    printf("[ENGINE] Server started on %s\n", g_sock_path);
    printf("[ENGINE] Chunk size: %d KB\n", CHUNK_SIZE / 1024);
    
    // حلقه اصلی
    while (1) {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }
        
        // ایجاد thread برای هر اتصال
        pthread_t thread;
        if (pthread_create(&thread, NULL, (void*(*)(void*))handle_connection,
                          (void*)(long)client_fd) != 0) {
            perror("pthread_create");
            close(client_fd);
            continue;
        }
        
        pthread_detach(thread);
    }
    
    // پاکسازی
    close(server_fd);
    unlink(g_sock_path);
    
    printf("[ENGINE] Server shutdown complete\n");
    return 0;
}