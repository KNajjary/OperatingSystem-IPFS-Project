// c_engine_linux.c - کامپایل: gcc -O2 -pthread -o c_engine c_engine_linux.c -lblake3
// اجرا: ./c_engine /tmp/cengine.sock

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

// تعریف ثابت‌ها
#define OP_UPLOAD_START    0x01
#define OP_UPLOAD_CHUNK    0x02
#define OP_UPLOAD_FINISH   0x03
#define OP_UPLOAD_DONE     0x81

#define OP_DOWNLOAD_START  0x11
#define OP_DOWNLOAD_CHUNK  0x91
#define OP_DOWNLOAD_DONE   0x92

#define CHUNK_SIZE     (256 * 1024)  // 256KB
#define HASH_LENGTH    32            // Blake3 hash size

static const char *g_sock_path = NULL;
static const char *BLOCKS_DIR = "blocks";
static const char *MANIFESTS_DIR = "manifests";

// ساختارها
struct file_part {
    int index;
    int size;
    unsigned char hash[HASH_LENGTH];
    char *file_path;
};

struct upload_info {
    char *filename;
    long long total_size;
    int chunk_count;
    struct file_part *chunks;
    int current_chunk;
};

// تابع کمکی برای ایجاد پوشه
void ensure_dir(const char *path) {
    struct stat st;
    if (stat(path, &st) == -1) {
        mkdir(path, 0755);
    }
}

// خواندن دقیق n بایت
ssize_t read_exact(int fd, void *buf, size_t n) {
    size_t total = 0;
    while (total < n) {
        ssize_t r = read(fd, (char*)buf + total, n - total);
        if (r == 0) return 0;  // EOF
        if (r < 0) {
            if (errno == EINTR) continue;
            perror("read");
            return -1;
        }
        total += r;
    }
    return total;
}

// نوشتن کامل n بایت
int write_all(int fd, const void *buf, size_t n) {
    size_t total = 0;
    while (total < n) {
        ssize_t w = write(fd, (const char*)buf + total, n - total);
        if (w < 0) {
            if (errno == EINTR) continue;
            perror("write");
            return -1;
        }
        total += w;
    }
    return 0;
}

// ارسال فریم با فرمت opcode + length + payload
int send_frame(int fd, uint8_t opcode, const void *payload, uint32_t length) {
    uint8_t header[5];
    header[0] = opcode;
    uint32_t be_len = htonl(length);
    memcpy(header + 1, &be_len, 4);
    
    if (write_all(fd, header, 5) < 0) return -1;
    if (length > 0 && write_all(fd, payload, length) < 0) return -1;
    
    return 0;
}

// محاسبه هش با Blake3
void compute_hash(const unsigned char *data, size_t len, unsigned char *hash) {
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data, len);
    blake3_hasher_finalize(&hasher, hash, HASH_LENGTH);
}

// ذخیره چانک در فایل‌سیستم
char* store_chunk(const unsigned char *data, size_t len, const unsigned char *hash) {
    // تبدیل هش به هگزادسیمال
    char hash_hex[65];
    for (int i = 0; i < HASH_LENGTH; i++) {
        sprintf(hash_hex + i * 2, "%02x", hash[i]);
    }
    hash_hex[64] = '\0';
    
    // ایجاد ساختار دایرکتوری دو سطحی
    char dir_path[256];
    char subdir_path[256];
    snprintf(dir_path, sizeof(dir_path), "%s/%.2s", BLOCKS_DIR, hash_hex);
    snprintf(subdir_path, sizeof(subdir_path), "%s/%.2s/%.2s", BLOCKS_DIR, hash_hex, hash_hex + 2);
    
    // ایجاد پوشه‌ها
    ensure_dir(BLOCKS_DIR);
    ensure_dir(dir_path);
    ensure_dir(subdir_path);
    
    // مسیر فایل نهایی
    char *filepath = malloc(300);
    if (!filepath) {
        perror("malloc");
        return NULL;
    }
    snprintf(filepath, 300, "%s/%s", subdir_path, hash_hex);
    
    // بررسی وجود فایل
    struct stat st;
    if (stat(filepath, &st) == 0) {
        printf("[ENGINE] Chunk exists: %s\n", hash_hex);
        return filepath;
    }
    
    // ایجاد فایل جدید
    int fd = open(filepath, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) {
        perror("open");
        free(filepath);
        return NULL;
    }
    
    // نوشتن داده
    if (write_all(fd, data, len) < 0) {
        close(fd);
        unlink(filepath);
        free(filepath);return NULL;
    }
    
    close(fd);
    printf("[ENGINE] Stored chunk: %s (size: %zu)\n", hash_hex, len);
    return filepath;
}

// کدگذاری base32 ساده‌شده
char* simple_base32(const unsigned char *data, size_t len) {
    const char *alphabet = "abcdefghijklmnopqrstuvwxyz234567";
    size_t out_len = (len * 8 + 4) / 5;
    char *encoded = malloc(out_len + 1);
    if (!encoded) return NULL;
    
    int buffer = 0;
    int bits = 0;
    int out_idx = 0;
    
    for (size_t i = 0; i < len; i++) {
        buffer = (buffer << 8) | data[i];
        bits += 8;
        
        while (bits >= 5) {
            int index = (buffer >> (bits - 5)) & 0x1F;
            encoded[out_idx++] = alphabet[index];
            bits -= 5;
        }
    }
    
    if (bits > 0) {
        int index = (buffer << (5 - bits)) & 0x1F;
        encoded[out_idx++] = alphabet[index];
    }
    
    encoded[out_idx] = '\0';
    return encoded;
}

// ایجاد مانیفست و CID
char* create_manifest(struct upload_info *state, char **manifest_json) {
    // محاسبه اندازه JSON
    size_t json_size = 1024;  // اندازه پایه
    for (int i = 0; i < state->chunk_count; i++) {
        json_size += 256;  // برای هر چانک
    }
    
    // تخصیص حافظه
    *manifest_json = malloc(json_size);
    if (!*manifest_json) {
        perror("malloc");
        return NULL;
    }
    
    // ایجاد JSON
    char *ptr = *manifest_json;
    int written = snprintf(ptr, json_size,
        "{\n"
        "  \"version\": 1,\n"
        "  \"hash_algo\": \"blake3\",\n"
        "  \"chunk_size\": %d,\n"
        "  \"total_size\": %lld,\n"
        "  \"filename\": \"%s\",\n"
        "  \"chunks\": [\n",
        CHUNK_SIZE, state->total_size, state->filename);
    ptr += written;
    
    // افزودن اطلاعات چانک‌ها
    for (int i = 0; i < state->chunk_count; i++) {
        char hash_hex[65];
        for (int j = 0; j < HASH_LENGTH; j++) {
            sprintf(hash_hex + j * 2, "%02x", state->chunks[i].hash[j]);
        }
        hash_hex[64] = '\0';
        
        written = snprintf(ptr, (*manifest_json + json_size) - ptr,
            "    {\"index\": %d, \"size\": %d, \"hash\": \"%s\"}%s\n",
            state->chunks[i].index, state->chunks[i].size, hash_hex,
            (i == state->chunk_count - 1) ? "" : ",");
        ptr += written;
    }
    
    // پایان JSON
    strcpy(ptr, "  ]\n}\n");
    
    // محاسبه CID ساده‌شده
    unsigned char manifest_hash[HASH_LENGTH];
    compute_hash((unsigned char*)*manifest_json, strlen(*manifest_json), manifest_hash);
    
    // ایجاد CID: b + base32(manifest_hash)
    char *base32 = simple_base32(manifest_hash, HASH_LENGTH);
    if (!base32) {
        free(*manifest_json);
        return NULL;
    }
    
    char *cid = malloc(strlen(base32) + 2);
    if (!cid) {
        free(base32);
        free(*manifest_json);
        return NULL;
    }
    
    sprintf(cid, "b%s", base32);
    free(base32);
    
    printf("[ENGINE] Generated CID: %s\n", cid);
    return cid;
}

// ذخیره مانیفست
int store_manifest(const char *cid, const char *manifest_json) {
    ensure_dir(MANIFESTS_DIR);
    
    // ایجاد مسیر فایل
    char manifest_path[256];
    snprintf(manifest_path, sizeof(manifest_path), "%s/%s.json", MANIFESTS_DIR, cid);
    
    // استفاده از فایل موقت برای نوشتن اتمیک
    char temp_path[256];
    snprintf(temp_path, sizeof(temp_path), "%s/%s.tmp", MANIFESTS_DIR, cid);
    
    // نوشتن به فایل موقت
    int fd = open(temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        perror("open");
        return -1;
    }
    
    if (write_all(fd, manifest_json, strlen(manifest_json)) < 0) {
        close(fd);
        unlink(temp_path);
        return -1;
    }
    
    fsync(fd);
    close(fd);
    
    // تغییر نام اتمیک
    if (rename(temp_path, manifest_path) < 0) {
        perror("rename");
        unlink(temp_path);
        return -1;
    }
    
    printf("[ENGINE] Manifest saved: %s\n", manifest_path);
    return 0;
}

// پاکسازی وضعیت آپلود
void cleanup_upload(struct upload_info *state) {
    if (state->filename) {
        free(state->filename);
        state->filename = NULL;
    }
    
    if (state->chunks) {
        for (int i = 0; i < state->chunk_count; i++) {
            if (state->chunks[i].file_path) {
                free(state->chunks[i].file_path);
            }
        }
        free(state->chunks);
        state->chunks = NULL;
    }
    
    state->chunk_count = 0;
    state->total_size = 0;
    state->current_chunk = 0;
}

// پردازش اتصال کلاینت
void handle_client(int client_fd) {
    struct upload_info upload_state = {0};
    unsigned char *chunk_buffer = NULL;
    
    printf("[ENGINE] New connection: fd=%d\n", client_fd);
    
    while (1) {
        // خواندن هدر (1 بایت opcode + 4 بایت length)
        uint8_t header[5];
        if (read_exact(client_fd, header, 5) <= 0) {
            break;
        }
        
        uint8_t opcode = header[0];
        uint32_t length;
        memcpy(&length, header + 1, 4);
        length = ntohl(length);
        
        // خواندن payload
        uint8_t *payload = NULL;
        if (length > 0) {
            payload = malloc(length);
            if (!payload) {
                perror("malloc payload");
                break;
            }
            if (read_exact(client_fd, payload, length) <= 0) {
                free(payload);
                break;
            }
        }
        
        // پردازش opcode
        if (opcode == OP_UPLOAD_START) {
            printf("[ENGINE] UPLOAD_START: filename=%.*s\n", (int)length, (char*)payload);
            
            // پاکسازی وضعیت قبلی
            cleanup_upload(&upload_state);
            
            // ذخیره نام فایل
            upload_state.filename = malloc(length + 1);
            if (!upload_state.filename) {
                perror("malloc filename");
                free(payload);
                break;
            }
            memcpy(upload_state.filename, payload, length);
            upload_state.filename[length] = '\0';
            
            // تخصیص بافر برای چانک‌ها
            chunk_buffer = malloc(CHUNK_SIZE);
            if (!chunk_buffer) {
                perror("malloc chunk buffer");
                free(payload);
                break;
            }
        }
        else if (opcode == OP_UPLOAD_CHUNK) {
            if (!upload_state.filename) {
                printf("[ENGINE] ERROR: Chunk without START\n");
                free(payload);
                break;
            }
            
            // محاسبه هش چانک
            unsigned char hash[HASH_LENGTH];
            compute_hash(payload, length, hash);
            
            // ذخیره چانک
            char *filepath = store_chunk(payload, length, hash);
            if (!filepath) {
                free(payload);
                break;
            }
            
            // افزودن به لیست چانک‌ها
            struct file_part *new_chunks = realloc(upload_state.chunks, 
                (upload_state.chunk_count + 1) * sizeof(struct file_part));
            if (!new_chunks) {
                perror("realloc chunks");
                free(filepath);
                free(payload);
                break;
            }
            
            upload_state.chunks = new_chunks;
            struct file_part *chunk = &upload_state.chunks[upload_state.chunk_count];
            
            chunk->index = upload_state.chunk_count;
            chunk->size = length;
            memcpy(chunk->hash, hash, HASH_LENGTH);
            chunk->file_path = filepath;
            
            upload_state.chunk_count++;
            upload_state.total_size += length;
            
            printf("[ENGINE] Processed chunk %d, size: %u\n", 
                   upload_state.chunk_count - 1, length);
        }
        else if (opcode == OP_UPLOAD_FINISH) {
            if (!upload_state.filename) {
                printf("[ENGINE] ERROR: FINISH without START\n");
                break;
            }
            
            printf("[ENGINE] UPLOAD_FINISH: %d chunks, total: %lld bytes\n",
                   upload_state.chunk_count, upload_state.total_size);