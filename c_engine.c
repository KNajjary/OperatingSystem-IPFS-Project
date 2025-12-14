// Build: gcc -O2 -pthread -o c_engine c_engine.c
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
#include "blake3/blake3.h"
//#include <openssl/sha.h>
#include <time.h>
#include "download.c"
#include "upload.c"'
#include "thread_pool_managment.c"
#include "locks_managment.c"

// ==================== تعریف ثابت‌ها ====================
#define OP_UPLOAD_START    0x01
#define OP_UPLOAD_CHUNK    0x02
#define OP_UPLOAD_FINISH   0x03
#define OP_UPLOAD_DONE     0x81

#define OP_DOWNLOAD_START  0x11
#define OP_DOWNLOAD_CHUNK  0x91
#define OP_DOWNLOAD_DONE   0x92

#define OP_ERROR           0xFF

/*کدهای خطا
E_BAD_CID
E_NOT_FOUND
E_HASH_MISMATCH
E_BUSY
E_PROTO
*/

#define CHUNCK_BITS         (256 * 1024)  // 256KB
#define HASH_LENGTH        32
#define MAX_THREADS        8
#define MAX_TASKS          100
#define CID_PREFIX         "b"

#define HASH_ALGO "blake3"
// ==================== متغیرهای سراسری ====================
static const char *g_sock_path = NULL;
static const char *BLOCKS_DIR = "blocks";
static const char *MANIFESTS_DIR = "manifests";



// جدول هش برای چانک‌ها (ساده‌شده)
static chunk_entry_t **chunk_table = NULL;
static int chunk_table_size = 0;
static pthread_mutex_t chunk_table_lock = PTHREAD_MUTEX_INITIALIZER;

// جدول مانیفست‌ها
static manifest_entry_t **manifest_table = NULL;
static int manifest_table_size = 0;
static pthread_mutex_t manifest_table_lock = PTHREAD_MUTEX_INITIALIZER;


// ==================== توابع خودشان ====================
// خواندن دقیق n بایت
ssize_t read_n(int fd, void *buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, (char*)buf + got, n - got);
        if (r == 0) return 0;
        if (r < 0) { if (errno == EINTR) continue; perror("read"); return -1; }
        got += r;
    }
    return (ssize_t)got;
}

// نوشتن کامل n بایت
int write_all(int fd, const void *buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        ssize_t w = write(fd, (const char*)buf + sent, n - sent);
        if (w < 0) { if (errno == EINTR) continue; perror("write"); return -1; }
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

// ==================== پردازش اتصال ====================

void handle_connection(int client_fd) {
    upload_state_t *upload_state = NULL;
    download_state_t *download_state = NULL;
   
    for (;;) {
        uint8_t header[5];
        if  (read_n(client_fd, header, 5) <= 0) break;
       
        uint8_t opcode = header[0];
        uint32_t length;
        memcpy(&length, header + 1, 4);
        length = ntohl(length);
       
        uint8_t *payload = NULL;
        if (length > 0) {
            payload = (uint8_t*)malloc(length);
            if (!payload) { perror("malloc"); break; }
            if (read_n(client_fd, payload, length) <= 0) { free(payload); break; }
        }
        
       
        if (opcode == OP_UPLOAD_START) {
            upload_state_t *state = handle_upload_start(client_fd, payload, length);
            // باید upload_state ایجاد و ذخیره شود
            handle_upload_chunk(client_fd, payload, length, upload_state);
            handle_upload_finish(client_fd, upload_state);
            // پاکسازی upload_state
            if (upload_state) {
                free(upload_state->filename);
                if (upload_state->chunks) {
                    for (int i = 0; i < upload_state->chunk_count; i++) {
                        free(upload_state->chunks[i].file_path);
                    }
                    free(upload_state->chunks);
                }
                pthread_mutex_destroy(&upload_state->lock);
                pthread_cond_destroy(&upload_state->all_chunks_done);
                free(upload_state);
                upload_state = NULL;  
            }  
        }       
        /*else if (opcode == OP_UPLOAD_CHUNK) {
            handle_upload_chunk(client_fd, payload, length, upload_state);

        } 
        else if (opcode == OP_UPLOAD_FINISH) {      
        
            handle_upload_finish(client_fd, upload_state);
            // پاکسازی upload_state
            if (upload_state) {
                free(upload_state->filename);
                if (upload_state->chunks) {
                    for (int i = 0; i < upload_state->chunk_count; i++) {
                        free(upload_state->chunks[i].file_path);
                    }
                    free(upload_state->chunks);
                }
                pthread_mutex_destroy(&upload_state->lock);
                pthread_cond_destroy(&upload_state->all_chunks_done);
                free(upload_state);
                upload_state = NULL;
            }
                }*/
               
        else if (opcode == OP_DOWNLOAD_START) {
            
            // =======استخراج CID از payload
            char *cid = malloc(len + 1);
            if (!cid)
            {
                printf("[ENGINE] ERROR: Failed to allocate CID\n");
                free(payload);
                break;
            }
            memcpy(cid, payload, len);
            cid[len] = '\0';
            free(payload);

            // =============اجرای دانلود در thread جداگانه برای پشتیبانی از همزمانی
            download_state_t * download_state =  download_start(client_fd, cid);
            download_chunks(download_state);
            download_done(download_state);


            free(cid);
            break;
        }  
        /*else if (opcode == OP_DOWNLOAD_CHUNK) {

        }   
        else if (opcode == OP_DOWNLOAD_DONE) {

        } */ 
        else{  
            printf("[ENGINE] Unknown opcode: 0x%02x\n", opcode);
            send_error(client_fd, "E_PROTO", "Invalid opcode");
                
        }
       
        if (payload) free(payload);
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
   
    // راه‌اندازی Thread Pool
    if (thread_pool_init(&thread_pool) < 0) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        return 1;
    }
   
    // ایجاد سوکت
    int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        thread_pool_shutdown(&thread_pool);
        return 1;
    }
   
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, g_sock_path, sizeof(addr.sun_path) - 1);
   
    // حذف فایل سوکت قبلی
    unlink(g_sock_path);
   
    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(server_fd);
        thread_pool_shutdown(&thread_pool);
        return 1;
    }
   
    if (listen(server_fd, 64) < 0) {
        perror("listen");
        close(server_fd);
        unlink(g_sock_path);
        thread_pool_shutdown(&thread_pool);
        return 1;
    }
   
    printf("[ENGINE] Server started on %s\n", g_sock_path);
    printf("[ENGINE] Thread pool: %d threads\n", MAX_THREADS);
    printf("[ENGINE] Chunk size: %d KB\n", CHUNCK_BITS / 1024);
   
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
    thread_pool_shutdown(&thread_pool);
   
    printf("[ENGINE] Server shutdown complete\n");
    return 0;
}
