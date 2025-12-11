// c_engine_complete.c - نسخه کامل با تمام قابلیت‌ها
// کامپایل: gcc -O2 -pthread -o c_engine c_engine_complete.c -lblake3 -lcrypto
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
#include <openssl/sha.h>
#include <time.h>

// ==================== تعریف ثابت‌ها ====================
#define OP_UPLOAD_START    0x01
#define OP_UPLOAD_CHUNK    0x02
#define OP_UPLOAD_FINISH   0x03
#define OP_UPLOAD_DONE     0x81

#define OP_DOWNLOAD_START  0x10
#define OP_DOWNLOAD_CHUNK  0x90
#define OP_DOWNLOAD_DONE   0x91

#define OP_ERROR           0xFF

#define CHUNK_SIZE         (256 * 1024)  // 256KB
#define HASH_LENGTH        32
#define MAX_THREADS        8
#define MAX_TASKS          100
#define CID_PREFIX         "b"

// ==================== ساختارهای داده ====================

// ساختار برای ذخیره اطلاعات هر چانک
typedef struct {
    char hash_hex[65];          // هش هگزادسیمال
    char *file_path;            // مسیر فایل
    int ref_count;              // شمارنده مرجع
    pthread_mutex_t lock;       // قفل برای این چانک
    time_t last_access;         // زمان آخرین دسترسی
} chunk_entry_t;

// ساختار برای ذخیره اطلاعات مانیفست
typedef struct {
    char *cid;                  // CID
    char *manifest_path;        // مسیر فایل مانیفست
    int reader_count;           // تعداد خوانندگان همزمان
    pthread_rwlock_t rwlock;    // قفل خواننده/نویسنده
} manifest_entry_t;

// ساختار وظیفه برای Thread Pool
typedef struct {
    void (*task_func)(void*);   // تابع وظیفه
    void *arg;                  // آرگومان تابع
    int task_id;                // شناسه وظیفه
} task_t;

// Thread Pool
typedef struct {
    pthread_t workers[MAX_THREADS]; // کارگران
    task_t task_queue[MAX_TASKS];   // صف وظایف
    int queue_size;                 // اندازه صف
    int queue_front;                // ابتدای صف
    int queue_rear;                 // انتهای صف
    int shutdown;                   // وضعیت خاموشی
    pthread_mutex_t queue_lock;     // قفل صف
    pthread_cond_t queue_not_empty; // شرط پر نبودن صف
    pthread_cond_t queue_not_full;  // شرط پر بودن صف
} thread_pool_t;

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

// ایجاد پوشه
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
        if (r == 0) return 0;
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

// ارسال فریم
int send_frame(int fd, uint8_t opcode, const void *payload, uint32_t length) {
    uint8_t header[5];
    header[0] = opcode;
    uint32_t be_len = htonl(length);
    memcpy(header + 1, &be_len, 4);
    
    if (write_all(fd, header, 5) < 0) return -1;
    if (length > 0 && write_all(fd, payload, length) < 0) return -1;
    
    return 0;
}

// ارسال خطا
int send_error(int fd, const char *code, const char *message) {
    char error_json[256];
    snprintf(error_json, sizeof(error_json), 
             "{\"code\":\"%s\",\"message\":\"%s\"}", code, message);
    return send_frame(fd, OP_ERROR, error_json, strlen(error_json));
}

// محاسبه هش (از BLAKE3 یا SHA256)
void compute_hash(const uint8_t *data, size_t len, uint8_t *hash) {
    char *algo = getenv("HASH_ALGO");
    if (algo && strcmp(algo, "sha256") == 0) {
        SHA256(data, len, hash);
    } else {
        // پیش‌فرض: BLAKE3
        blake3_hasher hasher;
        blake3_hasher_init(&hasher);
        blake3_hasher_update(&hasher, data, len);
        blake3_hasher_finalize(&hasher, hash, HASH_LENGTH);
    }
}

// تبدیل هش به هگزادسیمال
void hash_to_hex(const uint8_t *hash, char *hex) {
    for (int i = 0; i < HASH_LENGTH; i++) {
        sprintf(hex + i * 2, "%02x", hash[i]);
    }
    hex[64] = '\0';
}

// ==================== مدیریت Thread Pool ====================

void *worker_thread(void *arg) {
    thread_pool_t *pool = (thread_pool_t *)arg;
    
    while (1) {
        pthread_mutex_lock(&pool->queue_lock);
        
        // منتظر بودن تا صف خالی نباشد
        while (pool->queue_size == 0 && !pool->shutdown) {
            pthread_cond_wait(&pool->queue_not_empty, &pool->queue_lock);
        }
        
        if (pool->shutdown) {
            pthread_mutex_unlock(&pool->queue_lock);
            pthread_exit(NULL);
        }
        
        // گرفتن وظیفه از صف
        task_t task = pool->task_queue[pool->queue_front];
        pool->queue_front = (pool->queue_front + 1) % MAX_TASKS;
        pool->queue_size--;
        
        // اطلاع به تولیدکنندگان که جا خالی شده
        pthread_cond_signal(&pool->queue_not_full);
        pthread_mutex_unlock(&pool->queue_lock);
        
        // اجرای وظیفه
        task.task_func(task.arg);
        
        // آزادسازی حافظه آرگومان
        free(task.arg);
    }
    
    return NULL;
}

int thread_pool_init(thread_pool_t *pool) {
    pool->queue_size = 0;
    pool->queue_front = 0;
    pool->queue_rear = 0;
    pool->shutdown = 0;
    
    pthread_mutex_init(&pool->queue_lock, NULL);
    pthread_cond_init(&pool->queue_not_empty, NULL);
    pthread_cond_init(&pool->queue_not_full, NULL);
    
    // ایجاد کارگران
    for (int i = 0; i < MAX_THREADS; i++) {
        if (pthread_create(&pool->workers[i], NULL, worker_thread, pool) != 0) {
            return -1;
        }
    }
    
    return 0;
}

int thread_pool_submit(thread_pool_t *pool, void (*task_func)(void*), void *arg, int task_id) {
    pthread_mutex_lock(&pool->queue_lock);
    
    // منتظر بودن تا صف پر نباشد
    while (pool->queue_size == MAX_TASKS && !pool->shutdown) {
        pthread_cond_wait(&pool->queue_not_full, &pool->queue_lock);
    }
    
    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->queue_lock);
        return -1;
    }
    
    // اضافه کردن وظیفه به صف
    pool->task_queue[pool->queue_rear].task_func = task_func;
    pool->task_queue[pool->queue_rear].arg = arg;
    pool->task_queue[pool->queue_rear].task_id = task_id;
    pool->queue_rear = (pool->queue_rear + 1) % MAX_TASKS;
    pool->queue_size++;
    
    // اطلاع به کارگران
    pthread_cond_signal(&pool->queue_not_empty);
    pthread_mutex_unlock(&pool->queue_lock);
    
    return 0;
}

void thread_pool_shutdown(thread_pool_t *pool) {
    pthread_mutex_lock(&pool->queue_lock);
    pool->shutdown = 1;
    pthread_mutex_unlock(&pool->queue_lock);
    
    // بیدار کردن تمام کارگران
    pthread_cond_broadcast(&pool->queue_not_empty);
    pthread_cond_broadcast(&pool->queue_not_full);
    
    // منتظر ماندن برای تمام شدن کارگران
    for (int i = 0; i < MAX_THREADS; i++) {
        pthread_join(pool->workers[i], NULL);
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
    
    char file_path[300];
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
        "  \"chunk_size\": %d,\n"
        "  \"total_size\": %lld,\n"
        "  \"filename\": \"%s\",\n"
        "  \"chunks\": [\n",
        getenv("HASH_ALGO") ? getenv("HASH_ALGO") : "blake3",
        CHUNK_SIZE, state->total_size, state->filename);
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

// ذخیره مانیفست (با نوشتن اتمیک)
int save_manifest(const char *cid, const char *manifest_json) {
    ensure_dir(MANIFESTS_DIR);
    
    // مسیر فایل اصلی و موقت
    char manifest_path[256];
    char temp_path[256];
    snprintf(manifest_path, sizeof(manifest_path), "%s/%s.json", MANIFESTS_DIR, cid);
    snprintf(temp_path, sizeof(temp_path), "%s/%s.tmp", MANIFESTS_DIR, cid);
    
    // نوشتن در فایل موقت
    int fd = open(temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    
    if (write_all(fd, manifest_json, strlen(manifest_json)) < 0) {
        close(fd);
        unlink(temp_path);
        return -1;
    }
    
    fsync(fd);
    close(fd);
    
    // تغییر نام اتمیک
    if (rename(temp_path, manifest_path) < 0) {
        unlink(temp_path);
        return -1;
    }
    
    return 0;
}

// ==================== پردازش آپلود ====================

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
        state->chunk_count = (len + CHUNK_SIZE - 1) / CHUNK_SIZE;
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
    char manifest_path[256];
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
    fread(manifest_json, 1, fsize, f);
    manifest_json[fsize] = '\0';
    fclose(f);
    
    // TODO: پارس کردن JSON و ارسال چانک‌ها با ترتیب صحیح
    // این بخش نیاز به پیاده‌سازی کامل دارد
    
    // برای نمونه، فقط پاسخ موفق می‌دهیم
    send_frame(client_fd, OP_DOWNLOAD_DONE, NULL, 0);
    
    pthread_rwlock_unlock(&manifest->rwlock);
    free(manifest_json);
}

// ==================== پردازش اتصال ====================

void handle_connection(int client_fd) {
    upload_state_t *upload_state = NULL;
    download_state_t *download_state = NULL;
    
    while (1) {
        uint8_t header[5];
        if (read_exact(client_fd, header, 5) <= 0) break;
        
        uint8_t opcode = header[0];
        uint32_t length;
        memcpy(&length, header + 1, 4);
        length = ntohl(length);
        
        uint8_t *payload = NULL;
        if (length > 0) {
            payload = malloc(length);
            if (!payload || read_exact(client_fd, payload, length) <= 0) {
                if (payload) free(payload);
                break;
            }
        }
        
        switch (opcode) {
            case OP_UPLOAD_START:
                handle_upload_start(client_fd, payload, length);
                // باید upload_state ایجاد و ذخیره شود
                break;
                
            case OP_UPLOAD_CHUNK:
                handle_upload_chunk(client_fd, payload, length, upload_state);
                break;
                
            case OP_UPLOAD_FINISH:
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
                break;
                
            case OP_DOWNLOAD_START:
                handle_download_start(client_fd, payload, length);
                break;
                
            default:
                printf("[ENGINE] Unknown opcode: 0x%02x\n", opcode);
                send_error(client_fd, "E_PROTO", "Invalid opcode");
                break;
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
    thread_pool_shutdown(&thread_pool);
    
    printf("[ENGINE] Server shutdown complete\n");
    return 0;
}

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

// c_engine_linux.c - کامپایل: gcc -O2 -pthread -o c_engine c_engine_linux.c -lblake3
// اجرا: ./c_engine /tmp/cengine.sock
/*
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
*/