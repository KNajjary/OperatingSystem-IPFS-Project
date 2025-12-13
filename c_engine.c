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

// ==================== تعریف ثابت‌ها ====================
#define OP_UPLOAD_START    0x01
#define OP_UPLOAD_CHUNK    0x02
#define OP_UPLOAD_FINISH   0x03
#define OP_UPLOAD_DONE     0x81

#define OP_DOWNLOAD_START  0x11
#define OP_DOWNLOAD_CHUNK  0x91
#define OP_DOWNLOAD_DONE   0x92

#define OP_ERROR           0xFF

#define CHUNCK_BITS         (256 * 1024)  // 256KB
#define HASH_LENGTH        32
#define MAX_THREADS        8
#define MAX_TASKS          100
#define CID_PREFIX         "b"



// وضعیت آپلود برای هر اتصال
typedef struct {
    char *filename;                 // نام فایل
    long long total_size;           // اندازه کل
    int chunk_count;                // تعداد چانک‌ها
    struct {
        int index;                  // شماره چانک
        int size;                   // اندازه چانک
        unsigned char hash[HASH_LENGTH]; // هش
        char *file_path;            // مسیر فایل
    } *chunks;                      // آرایه چانک‌ها
    int chunks_received;            // چانک‌های دریافت شده
    pthread_mutex_t lock;           // قفل وضعیت
    pthread_cond_t all_chunks_done; // شرط اتمام چانک‌ها
} upload_state_t;

// وضعیت دانلود برای هر اتصال
typedef struct {
    char *cid;                      // CID
    manifest_entry_t *manifest;     // اشاره‌گر به مانیفست
    int total_chunks;               // تعداد کل چانک‌ها
    int chunks_sent;                // چانک‌های ارسال شده
    pthread_mutex_t lock;           // قفل وضعیت
} download_state_t;

// ==================== ساختارهای داده برای دانلود همزمان ====================
/*

*/

// 1. ساختار اطلاعات مانیفست با قفل خواننده/نویسنده
typedef struct manifest_info {
    char *cid;                      // CID (مثلاً "bciq2k...")
    char *manifest_json;            // محتوای JSON مانیفست
    int ref_count;                  // تعداد خوانندگان فعال همزمان
    pthread_rwlock_t rwlock;        // قفل خواننده/نویسنده برای دسترسی همزمان
    time_t last_access;             // زمان آخرین دسترسی (برای LRU cache)
    struct manifest_info *next;     // برای لیست پیوندی/هش‌تگل
    int chunk_count;                // تعداد چانک‌ها در این مانیفست
    char **chunk_hashes;            // آرایه هش‌های چانک‌ها
    int *CHUNCK_BITSs;               // آرایه اندازه‌های چانک‌ها
} manifest_info_t;

// 2. ساختار وضعیت دانلود برای هر اتصال کلاینت
/*typedef struct download_state {
    char *cid;                      // CID درخواستی
    int client_fd;                  // فایل‌دسکریپتور سوکت کلاینت
    manifest_info_t *manifest;      // اشاره‌گر به مانیفست مربوطه
    
    // اطلاعات چانک‌ها
    int total_chunks;               // تعداد کل چانک‌ها
    int *chunk_order;               // آرایه ترتیب ارسال چانک‌ها
    uint8_t **chunk_data;           // آرایه داده‌های چانک‌ها (اشاره‌گر)
    size_t *CHUNCK_BITSs;            // آرایه اندازه هر چانک
    int *chunk_ready;               // وضعیت آماده بودن (0=درحال پردازش، 1=آماده، 2=ارسال شده)
    char **chunk_paths;             // آرایه مسیر فایل‌های چانک
    
    // شمارنده‌ها و همگام‌سازی
    int chunks_ready_count;         // تعداد چانک‌های آماده
    int chunks_sent_count;          // تعداد چانک‌های ارسال شده
    int next_expected_index;        // اندیس بعدی که باید ارسال شود
    
    // همگام‌سازی
    pthread_mutex_t lock;           // قفل برای دسترسی به وضعیت
    pthread_cond_t chunk_ready_cond;// شرط برای آماده شدن چانک‌ها
    pthread_cond_t chunk_sent_cond; // شرط برای ارسال چانک‌ها
    
    // برای ادغام ترتیبی
    struct {
        int *pending_indices;       // اندیس‌های چانک‌های در انتظار
        int pending_count;          // تعداد چانک‌های در انتظار
        pthread_mutex_t output_lock;// قفل ادغام خروجی
    } merger;
} download_state_t;*/
// بعد از خط ۱۴۸، ساختار download_state_t کامل را اضافه کنید:

// 2. ساختار وضعیت دانلود برای هر اتصال کلاینت
typedef struct download_state {
    char *cid;                      // CID درخواستی
    int client_fd;                  // فایل‌دسکریپتور سوکت کلاینت
    manifest_info_t *manifest;      // اشاره‌گر به مانیفست مربوطه
    
    // اطلاعات چانک‌ها
    int total_chunks;               // تعداد کل چانک‌ها
    uint8_t **chunk_data;           // آرایه داده‌های چانک‌ها (اشاره‌گر)
    size_t *chunk_sizes;            // آرایه اندازه هر چانک
    int *chunk_ready;               // وضعیت آماده بودن (0=درحال پردازش، 1=آماده، 2=ارسال شده)
    
    // شمارنده‌ها و همگام‌سازی
    int chunks_ready_count;         // تعداد چانک‌های آماده
    int chunks_sent_count;          // تعداد چانک‌های ارسال شده
    int next_expected_index;        // اندیس بعدی که باید ارسال شود
    
    // همگام‌سازی
    pthread_mutex_t lock;           // قفل برای دسترسی به وضعیت
    pthread_cond_t chunk_ready_cond;// شرط برای آماده شدن چانک‌ها
} download_state_t;

// ==================== متغیرهای سراسری ====================
static const char *g_sock_path = NULL;
static const char *BLOCKS_DIR = "blocks";
static const char *MANIFESTS_DIR = "manifests";

// Thread Pool سراسری
static thread_pool_t thread_pool;

// جدول هش برای چانک‌ها (ساده‌شده)
static chunk_entry_t **chunk_table = NULL;
static int chunk_table_size = 0;
static pthread_mutex_t chunk_table_lock = PTHREAD_MUTEX_INITIALIZER;

// جدول مانیفست‌ها
static manifest_entry_t **manifest_table = NULL;
static int manifest_table_size = 0;
static pthread_mutex_t manifest_table_lock = PTHREAD_MUTEX_INITIALIZER;

// ==================== توابع کمکی ====================



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



// محاسبه هش (از BLAKE3 یا SHA256)
void compute_hash(const uint8_t *data, size_t len, uint8_t *hash) {
    /*char *algo = getenv("HASH_ALGO");
    if (algo && strcmp(algo, "sha256") == 0) {
        SHA256(data, len, hash);
    } else {
        // پیش‌فرض: BLAKE3
        blake3_hasher hasher;
        blake3_hasher_init(&hasher);
        blake3_hasher_update(&hasher, data, len);
        blake3_hasher_finalize(&hasher, hash, HASH_LENGTH);
    //}*/
    // مقدار اولیه هش
    uint32_t hash_value = 5381;  // مقدار اولیه مشهور در الگوریتم DJB2
    
    // محاسبه هش برای داده ورودی
    for (size_t i = 0; i < len; i++) {
        hash_value = ((hash_value << 5) + hash_value) + data[i];  // hash * 33 + data[i]
    }
    
    // تبدیل مقدار 32 بیتی هش به آرایه 32 بایتی
    // (می‌توانیم آن را تکرار کنیم یا با روش‌های دیگر ترکیب کنیم)
    for (int i = 0; i < HASH_LENGTH; i++) {
        // استفاده از بایت‌های مختلف از hash_value با اعمال شیفت
        hash[i] = (uint8_t)(hash_value >> ((i % 4) * 8));
        
        // برای افزایش پیچیدگی، می‌توانیم عملیات اضافی انجام دهیم
        hash[i] ^= (uint8_t)(i * 0x1F);  // XOR با یک مقدار ثابت
    }
}




// ==================== مدیریت چانک‌ها ====================

// ساختار آرگومان برای وظیفه پردازش چانک
typedef struct {
    int client_fd;
    int chunk_index;
    uint8_t *data;
    size_t data_size;
    upload_state_t *upload_state;
} chunk_task_arg_t;

// وظیفه پردازش چانک (برای Thread Pool)
void process_chunk_task(void *arg) {
    chunk_task_arg_t *task_arg = (chunk_task_arg_t *)arg;
    upload_state_t *state = task_arg->upload_state;
   
    // محاسبه هش
    uint8_t hash[HASH_LENGTH];
    compute_hash(task_arg->data, task_arg->data_size, hash);
   
    char hash_hex[65];
    hash_to_hex(hash, hash_hex);
   
    // ساخت مسیر فایل
    char dir_path[256];
    char subdir_path[256];
    snprintf(dir_path, sizeof(dir_path), "%s/%.2s", BLOCKS_DIR, hash_hex);
    snprintf(subdir_path, sizeof(subdir_path), "%s/%.2s/%.2s",
             BLOCKS_DIR, hash_hex, hash_hex + 2);
   
    ensure_dir(BLOCKS_DIR);
    ensure_dir(dir_path);
    ensure_dir(subdir_path);
   
    char file_path[300];//‘%s’ directive output may be truncated writing up to 64 bytes into a region of size between 44 and 299
    snprintf(file_path, sizeof(file_path), "%s/%s", subdir_path, hash_hex);
   
    // قفل جدول چانک‌ها
    pthread_mutex_lock(&chunk_table_lock);
   
    // بررسی وجود چانک
    chunk_entry_t *chunk = NULL;
    for (int i = 0; i < chunk_table_size; i++) {
        if (chunk_table[i] && strcmp(chunk_table[i]->hash_hex, hash_hex) == 0) {
            chunk = chunk_table[i];
            break;
        }
    }
   
    if (chunk) {
        // چانک وجود دارد - افزایش شمارنده مرجع
        pthread_mutex_lock(&chunk->lock);
        chunk->ref_count++;
        pthread_mutex_unlock(&chunk->lock);
        printf("[ENGINE] Duplicate chunk found, ref_count: %d\n", chunk->ref_count);
    } else {
        // ایجاد چانک جدید
        chunk = malloc(sizeof(chunk_entry_t));
        strcpy(chunk->hash_hex, hash_hex);
        chunk->file_path = strdup(file_path);
        chunk->ref_count = 1;
        chunk->last_access = time(NULL);
        pthread_mutex_init(&chunk->lock, NULL);
       
        // ذخیره فایل
        int fd = open(file_path, O_WRONLY | O_CREAT | O_EXCL, 0644);
        if (fd >= 0) {
            write_all(fd, task_arg->data, task_arg->data_size);
            close(fd);
            printf("[ENGINE] Stored new chunk: %s\n", hash_hex);
        }
       
        // اضافه کردن به جدول
        chunk_table = realloc(chunk_table, (chunk_table_size + 1) * sizeof(chunk_entry_t*));
        chunk_table[chunk_table_size++] = chunk;
    }
   
    pthread_mutex_unlock(&chunk_table_lock);
   
    // ذخیره اطلاعات در وضعیت آپلود
    pthread_mutex_lock(&state->lock);
    state->chunks[task_arg->chunk_index].index = task_arg->chunk_index;
    state->chunks[task_arg->chunk_index].size = task_arg->data_size;
    memcpy(state->chunks[task_arg->chunk_index].hash, hash, HASH_LENGTH);
    state->chunks[task_arg->chunk_index].file_path = strdup(file_path);
   
    state->chunks_received++;
    printf("[ENGINE] Chunk %d processed, %d/%d done\n",
           task_arg->chunk_index, state->chunks_received, state->chunk_count);
   
    // بررسی اتمام تمام چانک‌ها
    if (state->chunks_received == state->chunk_count) {
        pthread_cond_signal(&state->all_chunks_done);
    }
   
    pthread_mutex_unlock(&state->lock);
}

// ==================== مدیریت مانیفست‌ها ====================

// کدگذاری Base32 ساده‌شده
char *base32_encode(const uint8_t *data, size_t len) {
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

// تولید CID
char *generate_cid(const char *manifest_json) {
    // محاسبه هش مانیفست
    uint8_t manifest_hash[HASH_LENGTH];
    compute_hash((const uint8_t*)manifest_json, strlen(manifest_json), manifest_hash);
   
    // ساخت multihash: [کد-الگوریتم, طول-هش, هش]
    uint8_t multihash[2 + HASH_LENGTH];
    multihash[0] = 0x1e;        // کد BLAKE3 در multihash
    multihash[1] = HASH_LENGTH;
    memcpy(multihash + 2, manifest_hash, HASH_LENGTH);
   
    // ساخت multicodec + multihash
    uint8_t multicodec_data[2 + 2 + HASH_LENGTH];
    unsigned int dag_json_code = 0x0129; // کد dag-json
   
    // کدگذاری Varint
    multicodec_data[0] = (dag_json_code >> 7) | 0x80;
    multicodec_data[1] = dag_json_code & 0x7F;
    memcpy(multicodec_data + 2, multihash, 2 + HASH_LENGTH);
   
    // کدگذاری Base32
    char *base32 = base32_encode(multicodec_data, 2 + 2 + HASH_LENGTH);
    if (!base32) return NULL;
   
    char *cid = malloc(strlen(base32) + 2);
    sprintf(cid, "b%s", base32);
    free(base32);
   
    return cid;
}

// ایجاد مانیفست
char *create_manifest(upload_state_t *state, char **manifest_json) {
    // محاسبه اندازه JSON
    size_t json_size = 1024;
    for (int i = 0; i < state->chunk_count; i++) {
        json_size += 256;
    }
   
    *manifest_json = malloc(json_size);
    if (!*manifest_json) return NULL;
   
    char *ptr = *manifest_json;
    int written = snprintf(ptr, json_size,
        "{\n"
        "  \"version\": 1,\n"
        "  \"hash_algo\": \"%s\",\n"
        "  \"CHUNCK_BITS\": %d,\n"
        "  \"total_size\": %lld,\n"
        "  \"filename\": \"%s\",\n"
        "  \"chunks\": [\n",
        getenv("HASH_ALGO") ? getenv("HASH_ALGO") : "blake3",
        CHUNCK_BITS, state->total_size, state->filename);
    ptr += written;
   
    // افزودن اطلاعات چانک‌ها
    for (int i = 0; i < state->chunk_count; i++) {
        char hash_hex[65];
        hash_to_hex(state->chunks[i].hash, hash_hex);
       
        written = snprintf(ptr, (*manifest_json + json_size) - ptr,
            "    {\"index\": %d, \"size\": %d, \"hash\": \"%s\"}%s\n",
            i, state->chunks[i].size, hash_hex,
            (i == state->chunk_count - 1) ? "" : ",");
        ptr += written;
    }
   
    strcpy(ptr, "  ]\n}\n");
   
    // تولید CID
    return generate_cid(*manifest_json);
}

// ==================== پردازش آپلود ====================
//
void handle_upload_start(int client_fd, const uint8_t *payload, uint32_t len) {
    printf("[ENGINE] UPLOAD_START: %.*s\n", (int)len, payload);
   
    // ایجاد وضعیت آپلود
    upload_state_t *state = malloc(sizeof(upload_state_t));
    state->filename = malloc(len + 1);
    memcpy(state->filename, payload, len);
    state->filename[len] = '\0';
    state->total_size = 0;
    state->chunk_count = 0;
    state->chunks_received = 0;
    state->chunks = NULL;
    pthread_mutex_init(&state->lock, NULL);
    pthread_cond_init(&state->all_chunks_done, NULL);
   
    // ذخیره در حافظه اتصال
    // (در واقعیت باید در یک ساختار connection ذخیره شود)
}

void handle_upload_chunk(int client_fd, const uint8_t *payload, uint32_t len,
                         upload_state_t *state) {
    if (!state) {
        send_error(client_fd, "E_PROTO", "No upload started");
        return;
    }
   
    pthread_mutex_lock(&state->lock);
   
    // تخصیص حافظه برای چانک جدید
    if (state->chunks == NULL) {
        // تخمین تعداد چانک‌ها
        state->chunk_count = (len + CHUNCK_BITS - 1) / CHUNCK_BITS;
        state->chunks = malloc(state->chunk_count * sizeof(state->chunks[0]));
        memset(state->chunks, 0, state->chunk_count * sizeof(state->chunks[0]));
    }
   
    // ایجاد آرگومان وظیفه
    chunk_task_arg_t *task_arg = malloc(sizeof(chunk_task_arg_t));
    task_arg->client_fd = client_fd;
    task_arg->chunk_index = state->chunks_received;
    task_arg->data = malloc(len);
    memcpy(task_arg->data, payload, len);
    task_arg->data_size = len;
    task_arg->upload_state = state;
   
    // ارسال به Thread Pool
    if (thread_pool_submit(&thread_pool, process_chunk_task, task_arg,
                           state->chunks_received) < 0) {
        free(task_arg->data);
        free(task_arg);
        send_error(client_fd, "E_BUSY", "Thread pool is busy");
    }
   
    state->total_size += len;
    pthread_mutex_unlock(&state->lock);
}

void handle_upload_finish(int client_fd, upload_state_t *state) {
    if (!state) {
        send_error(client_fd, "E_PROTO", "No upload started");
        return;
    }
   
    // منتظر ماندن برای اتمام تمام چانک‌ها
    pthread_mutex_lock(&state->lock);
    while (state->chunks_received < state->chunk_count) {
        pthread_cond_wait(&state->all_chunks_done, &state->lock);
    }
   
    // ایجاد مانیفست
    char *manifest_json = NULL;
    char *cid = create_manifest(state, &manifest_json);
   
    if (!cid || !manifest_json) {
        send_error(client_fd, "E_INTERNAL", "Failed to create manifest");
        pthread_mutex_unlock(&state->lock);
        return;
    }
   
    // ذخیره مانیفست
    if (save_manifest(cid, manifest_json) < 0) {
        send_error(client_fd, "E_INTERNAL", "Failed to save manifest");
        free(cid);
        free(manifest_json);
        pthread_mutex_unlock(&state->lock);
        return;
    }
   
    // ثبت در جدول مانیفست‌ها
    pthread_mutex_lock(&manifest_table_lock);
    manifest_entry_t *manifest = malloc(sizeof(manifest_entry_t));
    manifest->cid = strdup(cid);
    manifest->manifest_path = NULL; // می‌توان ذخیره کرد
    manifest->reader_count = 0;
    pthread_rwlock_init(&manifest->rwlock, NULL);
   
    manifest_table = realloc(manifest_table,
                            (manifest_table_size + 1) * sizeof(manifest_entry_t*));
    manifest_table[manifest_table_size++] = manifest;
    pthread_mutex_unlock(&manifest_table_lock);
   
    // ارسال پاسخ
    send_frame(client_fd, OP_UPLOAD_DONE, cid, strlen(cid));
   
    printf("[ENGINE] Upload completed: CID=%s, size=%lld, chunks=%d\n",
           cid, state->total_size, state->chunk_count);
   
    // پاکسازی
    free(cid);
    free(manifest_json);
    pthread_mutex_unlock(&state->lock);
   
    // پاکسازی وضعیت آپلود
    // (باید بعداً انجام شود)
}

// ==================== پردازش دانلود ====================

manifest_entry_t *find_manifest(const char *cid) {
    pthread_mutex_lock(&manifest_table_lock);
    for (int i = 0; i < manifest_table_size; i++) {
        if (manifest_table[i] && strcmp(manifest_table[i]->cid, cid) == 0) {
            pthread_mutex_unlock(&manifest_table_lock);
            return manifest_table[i];
        }
    }
    pthread_mutex_unlock(&manifest_table_lock);
    return NULL;
}
// تابع handle_download_start را کامل جایگزین کنید (حدود خط ۷۰۰):

// complete
void handle_download_start(int client_fd, const uint8_t *payload, uint32_t len) {
    char cid[300];  // افزایش اندازه برای جلوگیری از truncation
    if (len >= sizeof(cid)) len = sizeof(cid) - 1;
    memcpy(cid, payload, len);
    cid[len] = '\0';
    
    printf("[ENGINE] DOWNLOAD_START: CID=%s\n", cid);
    
    // ۱. خواندن مانیفست از فایل
    char manifest_path[350];  // افزایش اندازه
    snprintf(manifest_path, sizeof(manifest_path), "%s/%s.json", MANIFESTS_DIR, cid);
    
    FILE *f = fopen(manifest_path, "r");
    if (!f) {
        send_error(client_fd, "E_NOT_FOUND", "Manifest file not found");
        return;
    }
    
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    char *manifest_json = malloc(fsize + 1);
    size_t bytes_read = fread(manifest_json, 1, fsize, f);
    if (bytes_read != fsize) {
        free(manifest_json);
        fclose(f);
        send_error(client_fd, "E_INTERNAL", "Failed to read manifest");
        return;
    }
    manifest_json[fsize] = '\0';
    fclose(f);
    
    // ۲. پارس کردن JSON
    int chunk_count = 0;
    char **chunk_hashes = NULL;
    int *chunk_sizes = NULL;
    
    if (parse_manifest_json_simple(manifest_json, &chunk_count, &chunk_hashes, &chunk_sizes) != 0) {
        free(manifest_json);
        send_error(client_fd, "E_INTERNAL", "Failed to parse manifest");
        return;
    }
    
    printf("[ENGINE] Found %d chunks in manifest\n", chunk_count);
    
    // ۳. ایجاد وضعیت دانلود
    download_state_t *dl_state = create_download_state(cid, client_fd, chunk_count);
    if (!dl_state) {
        free(manifest_json);
        for (int i = 0; i < chunk_count; i++) free(chunk_hashes[i]);
        free(chunk_hashes);
        free(chunk_sizes);
        send_error(client_fd, "E_INTERNAL", "Failed to create download state");
        return;
    }
    
    // ۴. ایجاد وظایف برای Thread Pool
    for (int i = 0; i < chunk_count; i++) {
        // ساخت مسیر فایل چانک
        char *chunk_path = malloc(350);
        snprintf(chunk_path, 350, "%s/%.2s/%.2s/%s", 
                 BLOCKS_DIR, chunk_hashes[i], chunk_hashes[i] + 2, chunk_hashes[i]);
        
        // ایجاد وظیفه
        read_chunk_task_t *task = malloc(sizeof(read_chunk_task_t));
        task->dl_state = dl_state;
        task->chunk_index = i;
        task->chunk_hash = strdup(chunk_hashes[i]);
        task->chunk_path = chunk_path;
        task->expected_size = chunk_sizes[i];
        task->next = NULL;
        
        // ارسال به Thread Pool
        if (thread_pool_submit(&thread_pool, read_chunk_worker, task, i) < 0) {
            printf("[ENGINE] Failed to submit chunk %d to thread pool\n", i);
            free(task->chunk_hash);
            free(task->chunk_path);
            free(task);
        }
        
        free(chunk_hashes[i]);
    }
    
    // ۵. حلقه ارسال ترتیبی
    int next_to_send = 0;
    while (next_to_send < chunk_count) {
        pthread_mutex_lock(&dl_state->lock);
        
        // منتظر بمان تا چانک بعدی آماده شود
        while (dl_state->chunk_ready[next_to_send] == 0) {
            printf("[ENGINE] Waiting for chunk %d... (ready: %d/%d)\n", 
                   next_to_send, dl_state->chunks_ready_count, chunk_count);
            pthread_cond_wait(&dl_state->chunk_ready_cond, &dl_state->lock);
        }
        
        // چانک آماده است - ارسال کن
        printf("[ENGINE] Sending chunk %d (size: %zu)\n", 
               next_to_send, dl_state->chunk_sizes[next_to_send]);
        
        if (send_frame(client_fd, OP_DOWNLOAD_CHUNK, 
                      dl_state->chunk_data[next_to_send],
                      dl_state->chunk_sizes[next_to_send]) < 0) {
            pthread_mutex_unlock(&dl_state->lock);
            printf("[ENGINE] Failed to send chunk %d\n", next_to_send);
            break;
        }
        
        next_to_send++;
        dl_state->chunks_sent_count = next_to_send;
        dl_state->next_expected_index = next_to_send;
        
        pthread_mutex_unlock(&dl_state->lock);
    }
    
    // ۶. پایان دانلود
    if (next_to_send == chunk_count) {
        printf("[ENGINE] Download completed, sending DONE\n");
        send_frame(client_fd, OP_DOWNLOAD_DONE, NULL, 0);
    } else {
        printf("[ENGINE] Download incomplete (%d/%d)\n", next_to_send, chunk_count);
    }
    
    // ۷. پاکسازی
    free(manifest_json);
    free(chunk_hashes);
    free(chunk_sizes);
    free_download_state(dl_state);
}
void handle_download_start(int client_fd, const uint8_t *payload, uint32_t len) {
    char cid[256];
    if (len >= sizeof(cid)) len = sizeof(cid) - 1;
    memcpy(cid, payload, len);
    cid[len] = '\0';
   
    printf("[ENGINE] DOWNLOAD_START: CID=%s\n", cid);
   
    // یافتن مانیفست
    manifest_entry_t *manifest = find_manifest(cid);
    if (!manifest) {
        send_error(client_fd, "E_NOT_FOUND", "CID not found");
        return;
    }
   
    // قفل خواننده
    pthread_rwlock_rdlock(&manifest->rwlock);
   
    // خواندن مانیفست از فایل
    char manifest_path[256]; //warning: ‘%s’ directive output may be truncated writing up to 255 bytes into a region of size 246
    snprintf(manifest_path, sizeof(manifest_path), "%s/%s.json", MANIFESTS_DIR, cid);
   
    FILE *f = fopen(manifest_path, "r");
    if (!f) {
        pthread_rwlock_unlock(&manifest->rwlock);
        send_error(client_fd, "E_NOT_FOUND", "Manifest file not found");
        return;
    }
   
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
   
    char *manifest_json = malloc(fsize + 1);
    size_t bytes_read = fread(manifest_json, 1, fsize, f);
    if (bytes_read != fsize) {
        // مدیریت خطا
        free(manifest_json);
        fclose(f);
        return;
    }
    manifest_json[fsize] = '\0';
    fclose(f);
   
    // TODO: پارس کردن JSON و ارسال چانک‌ها با ترتیب صحیح
    // این بخش نیاز به پیاده‌سازی کامل دارد
   
    // برای نمونه، فقط پاسخ موفق می‌دهیم
    send_frame(client_fd, OP_DOWNLOAD_DONE, NULL, 0);
   
    pthread_rwlock_unlock(&manifest->rwlock);
    free(manifest_json);
}
// بعد از تابع hash_to_hex (حدود خط ۳۵۰) اضافه کنید:

// ==================== توابع کمکی برای دانلود ====================

// تابع ایجاد وضعیت دانلود
download_state_t* create_download_state(const char *cid, int client_fd, int total_chunks) {
    download_state_t *state = malloc(sizeof(download_state_t));
    if (!state) return NULL;
    
    memset(state, 0, sizeof(download_state_t));
    
    state->cid = strdup(cid);
    state->client_fd = client_fd;
    state->total_chunks = total_chunks;
    state->next_expected_index = 0;
    state->chunks_ready_count = 0;
    state->chunks_sent_count = 0;
    
    // تخصیص آرایه‌ها
    state->chunk_data = calloc(total_chunks, sizeof(uint8_t*));
    state->chunk_sizes = calloc(total_chunks, sizeof(size_t));
    state->chunk_ready = calloc(total_chunks, sizeof(int));
    
    // مقداردهی اولیه mutex و condition
    pthread_mutex_init(&state->lock, NULL);
    pthread_cond_init(&state->chunk_ready_cond, NULL);
    
    return state;
}

// تابع پاکسازی وضعیت دانلود
void free_download_state(download_state_t *state) {
    if (!state) return;
    
    free(state->cid);
    
    // آزادسازی آرایه‌های چانک‌ها
    if (state->chunk_data) {
        for (int i = 0; i < state->total_chunks; i++) {
            if (state->chunk_data[i]) {
                free(state->chunk_data[i]);
            }
        }
        free(state->chunk_data);
    }
    
    if (state->chunk_sizes) free(state->chunk_sizes);
    if (state->chunk_ready) free(state->chunk_ready);
    
    // تخریب mutex و condition
    pthread_mutex_destroy(&state->lock);
    pthread_cond_destroy(&state->chunk_ready_cond);
    
    free(state);
}
// بعد از تابع free_download_state اضافه کنید:

// پارس کردن ساده JSON مانیفست
int parse_manifest_json_simple(const char *json_str, 
                               int *chunk_count, 
                               char ***chunk_hashes,
                               int **chunk_sizes) {
    // این یک پارسر ساده است - فقط هش‌ها را استخراج می‌کند
    const char *pos = json_str;
    int count = 0;
    
    // شمردن تعداد "hash"
    while ((pos = strstr(pos, "\"hash\"")) != NULL) {
        count++;
        pos += 6;
    }
    
    if (count == 0) {
        *chunk_count = 0;
        *chunk_hashes = NULL;
        *chunk_sizes = NULL;
        return 0;
    }
    
    // تخصیص حافظه
    *chunk_count = count;
    *chunk_hashes = malloc(count * sizeof(char*));
    *chunk_sizes = malloc(count * sizeof(int));
    
    // استخراج هش‌ها
    pos = json_str;
    int index = 0;
    
    while ((pos = strstr(pos, "\"hash\"")) != NULL) {
        // حرکت به بعد از :
        pos = strchr(pos, ':');
        if (!pos) break;
        pos++; // بعد از :
        
        // پیدا کردن شروع هش (بعد از ")
        while (*pos && *pos != '\"') pos++;
        if (*pos != '\"') break;
        pos++; // بعد از "
        
        const char *hash_start = pos;
        
        // پیدا کردن پایان هش (قبل از ")
        while (*pos && *pos != '\"') pos++;
        if (*pos != '\"') break;
        
        int hash_len = pos - hash_start;
        (*chunk_hashes)[index] = malloc(hash_len + 1);
        strncpy((*chunk_hashes)[index], hash_start, hash_len);
        (*chunk_hashes)[index][hash_len] = '\0';
        
        // اندازه پیش‌فرض
        (*chunk_sizes)[index] = CHUNK_SIZE;
        
        index++;
        pos++;
    }
    
    return 0;
}
// بعد از تابع parse_manifest_json_simple اضافه کنید:

// تابع کارگر برای خواندن چانک
void read_chunk_worker(void *arg) {
    read_chunk_task_t *task = (read_chunk_task_t*)arg;
    download_state_t *dl_state = task->dl_state;
    
    printf("[DOWNLOAD WORKER] شروع خواندن چانک %d، هش: %s\n", 
           task->chunk_index, task->chunk_hash);
    
    // ۱. باز کردن فایل
    int fd = open(task->chunk_path, O_RDONLY);
    if (fd < 0) {
        printf("[DOWNLOAD WORKER] خطا: نتوانستم فایل %s را باز کنم\n", task->chunk_path);
        goto cleanup;
    }
    
    // ۲. گرفتن اندازه فایل
    struct stat st;
    if (fstat(fd, &st) < 0) {
        printf("[DOWNLOAD WORKER] خطا: نتوانستم اندازه فایل را بگیرم\n");
        close(fd);
        goto cleanup;
    }
    
    size_t file_size = st.st_size;
    
    // ۳. خواندن فایل
    uint8_t *data = malloc(file_size);
    if (!data) {
        printf("[DOWNLOAD WORKER] خطا: نتوانستم حافظه اختصاص بدهم\n");
        close(fd);
        goto cleanup;
    }
    
    ssize_t bytes_read = read(fd, data, file_size);
    close(fd);
    
    if (bytes_read != file_size) {
        printf("[DOWNLOAD WORKER] خطا: خواندن ناقص (انتظار: %zu, خوانده: %zd)\n", 
               file_size, bytes_read);
        free(data);
        goto cleanup;
    }
    
    // ۴. تأیید هش
    uint8_t computed_hash[HASH_LENGTH];
    compute_hash(data, file_size, computed_hash);
    
    char computed_hash_hex[65];
    hash_to_hex(computed_hash, computed_hash_hex);
    
    if (strcmp(computed_hash_hex, task->chunk_hash) != 0) {
        printf("[DOWNLOAD WORKER] خطا: هش مطابقت ندارد!\n");
        printf("  انتظار: %s\n", task->chunk_hash);
        printf("  محاسبه: %s\n", computed_hash_hex);
        free(data);
        goto cleanup;
    }
    
    printf("[DOWNLOAD WORKER] چانک %d تأیید شد، اندازه: %zu\n", 
           task->chunk_index, file_size);
    
    // ۵. ذخیره در وضعیت دانلود
    pthread_mutex_lock(&dl_state->lock);
    
    dl_state->chunk_data[task->chunk_index] = data;
    dl_state->chunk_sizes[task->chunk_index] = file_size;
    dl_state->chunk_ready[task->chunk_index] = 1;
    dl_state->chunks_ready_count++;
    
    // ۶. اگر این چانک، چانک بعدی مورد انتظار است، بیدار کن
    if (task->chunk_index == dl_state->next_expected_index) {
        printf("[DOWNLOAD WORKER] چانک %d آماده است (چانک بعدی مورد انتظار)\n", 
               task->chunk_index);
        pthread_cond_signal(&dl_state->chunk_ready_cond);
    }
    
    pthread_mutex_unlock(&dl_state->lock);
    
cleanup:
    // پاکسازی وظیفه
    free(task->chunk_hash);
    free(task->chunk_path);
    free(task);
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
            handle_upload_start(client_fd, payload, length);
            // باید upload_state ایجاد و ذخیره شود
                
        }       
        else if (opcode == OP_UPLOAD_CHUNK) {
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
                }
               
        else if (opcode == OP_DOWNLOAD_START) {
            handle_download_start(client_fd, payload, length);
                
        }       
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
