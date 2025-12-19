// Build: gcc -O2 -pthread -o c_engine c_engine.c -w -lssl -lcrypto
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
#include <time.h>
#include <stdatomic.h>
#include <openssl/sha.h>
#include <dirent.h>

// ==================== تعریف ثابت‌ها ====================
#define OP_UPLOAD_START    0x01
#define OP_UPLOAD_CHUNK    0x02
#define OP_UPLOAD_FINISH   0x03
#define OP_UPLOAD_DONE     0x81

#define OP_DOWNLOAD_START  0x11//0x10
#define OP_DOWNLOAD_CHUNK  0x91//0x90
#define OP_DOWNLOAD_DONE   0x92//0x91

#define OP_ERROR           0xFF

#define MAX_PATH_LENGTH 512
#define MAX_MANIFEST_SIZE (10 * 1024 * 1024)

#define CID_MAX_LEN 128
#define CHUNK_SIZE (256 * 1024)  // 256KB
#define HASH_LENGTH 32           // SHA256
#define MAX_THREADS 8
#define MAX_TASKS 100
#define HASH_ALGO "sha256"
#define LOCK_TABLE_SIZE 64

#define LOCK_SUCCESS 0
#define LOCK_FAILED -1
#define LOCK_TIMEOUT -2
#define LOCK_NO_MEMORY -3

// کدهای خطا
#define E_BAD_CID "E_BAD_CID"
#define E_NOT_FOUND "E_NOT_FOUND"
#define E_HASH_MISMATCH "E_HASH_MISMATCH"
#define E_BUSY "E_BUSY"
#define E_PROTO "E_PROTO"

// ==================== ساختارهای داده ====================

// ساختار برای ذخیره اطلاعات هر چانک
typedef struct chunk_entry {
    char hash_hex[65];
    char *file_path;
    int ref_count;
    pthread_mutex_t lock;
    time_t last_access;
    int marked_for_deletion;
} chunk_entry_t;

// ساختار اطلاعات فایل
typedef struct file_part {
    int index;
    int size;
    unsigned char hash[HASH_LENGTH];
    char *file_path;
} file_part_t;

// ساختار اطلاعات مانیفست
typedef struct manifest_info {
    char *filename;
    long long total_size;
    int chunk_count;
    file_part_t *chunks;
} manifest_info_t;

// ساختار entry برای مانیفست‌ها
typedef struct manifest_entry {
    char cid[128];
    manifest_info_t *manifest_info;
    int ref_count;
    time_t last_access;
    pthread_mutex_t lock;
    int marked_for_deletion;
} manifest_entry_t;

// ساختار entry برای قفل
typedef struct manifest_lock_entry {
    char cid[CID_MAX_LEN];
    pthread_rwlock_t rwlock;
    int ref_count;
    struct manifest_lock_entry *next;
} manifest_lock_entry_t;

// ساختار وضعیت آپلود
typedef struct upload_state {
    char filename[256];
    long long total_size;
    int chunk_count;
    int chunk_size;
    char hash_algo[32];
    struct {
        int index;
        int size;
        unsigned char hash[HASH_LENGTH];
        char *file_path;
    } *chunks;
    int chunks_received;
    int chunks_processed;
    pthread_mutex_t lock;
    pthread_cond_t all_chunks_done;
    int client_fd;
} upload_state_t;

// ساختار وضعیت دانلود
typedef struct download_state {
    char cid[128];
    int client_fd;
    manifest_info_t *manifest;
    int total_chunks;
    uint8_t **chunk_data;
    size_t *chunk_sizes;
    int *chunk_ready;
    char **chunk_paths;
    int *chunk_order;
    int chunks_ready_count;
    int chunks_sent_count;
    int next_expected_index;
    pthread_mutex_t lock;
    pthread_cond_t chunk_ready_cond;
    pthread_cond_t chunk_sent_cond;
    pthread_cond_t all_tasks_done_cond;  // اضافه شد
    int tasks_remaining;  // اضافه شد
} download_state_t;

// انواع وظایف
typedef enum {
    TASK_TYPE_UPLOAD_CHUNK,
    TASK_TYPE_DOWNLOAD_CHUNK,
    TASK_TYPE_COMPUTE_HASH,
    TASK_TYPE_WRITE_FILE,
    TASK_TYPE_DELETE_FILE
} task_type_t;

// ساختار وظیفه
typedef struct task {
    task_type_t type;
    union {
        struct {
            upload_state_t *upload_state;
            int chunk_index;
            uint8_t *data;
            size_t data_size;
        } upload;
        struct {
            download_state_t *download_state;
            int chunk_index;
            char chunk_hash[65];
            char chunk_path[512];
            size_t expected_size;
        } download;
        struct {
            uint8_t *data;
            size_t data_size;
            uint8_t hash[HASH_LENGTH];
        } hash;
        struct {
            char *file_path;
            uint8_t *data;
            size_t data_size;
        } write_file;
        struct {
            char *file_path;
        } delete_file;
    } data;
    void (*callback)(void*, int);
    void *callback_arg;
    int task_id;
    struct task *next;
} task_t;

// Thread Pool
typedef struct {
    pthread_t *workers;
    int worker_count;
    int max_workers;
    task_t *task_queue_head;
    task_t *task_queue_tail;
    int queue_size;
    int max_queue_size;
    int shutdown;
    int active_tasks;
    pthread_mutex_t queue_lock;
    pthread_cond_t queue_not_empty;
    pthread_cond_t queue_not_full;
    pthread_cond_t queue_empty;
    unsigned long tasks_completed;
    unsigned long tasks_failed;
    atomic_int next_task_id;
} thread_pool_t;

// جدول قفل‌ها
typedef struct {
    manifest_lock_entry_t *locks[LOCK_TABLE_SIZE];
    pthread_mutex_t table_lock;
} manifest_lock_table_t;

// ==================== متغیرهای سراسری ====================
static const char *g_sock_path = NULL;
static const char *BLOCKS_DIR = "blocks";
static const char *MANIFESTS_DIR = "manifests";

static manifest_entry_t **manifest_table = NULL;
static int manifest_table_size = 0;
static pthread_mutex_t manifest_table_lock = PTHREAD_MUTEX_INITIALIZER;

static chunk_entry_t **chunk_table = NULL;
static int chunk_table_size = 0;
static pthread_mutex_t chunk_table_lock = PTHREAD_MUTEX_INITIALIZER;

static thread_pool_t g_thread_pool;
static manifest_lock_table_t g_lock_table;

// ==================== توابع کمکی ====================

// ایجاد پوشه‌های تو در تو /aa/bb/
void ensure_dir(const char *path) {
    char tmp[512]; // بافر موقت برای ذخیره مسیر
    char *p = NULL; // اشاره‌گر برای پیمایش رشته
    size_t len; // طول رشته
    
    snprintf(tmp, sizeof(tmp), "%s", path);//write path in temp
    len = strlen(tmp);
    // اگر مسیر به / ختم شود، آن را حذف می‌کند
    // "/home/user/" → "/home/user"
    if (tmp[len - 1] == '/') { 
        tmp[len - 1] = 0; // null
    }
    
    for (p = tmp + 1; *p; p++) { //skip first /
        if (*p == '/') {
            *p = 0; //0 is ASCI code for NULL
            mkdir(tmp, 0755);//0755 is permissions
            *p = '/';//put it back
        }
    }
    mkdir(tmp, 0755);//final folder, we removed final / (or it didnt exest at the first place) so it is not generated in for loop
}

//===================================== pre defined ===========================
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
// ارسال فریم
/*int send_frame(int fd, uint8_t op, const void* payload, uint32_t len) {
    uint8_t header[5];
    header[0] = op;
    uint32_t be_len = htonl(len);
    memcpy(header + 1, &be_len, 4);
    if (write_all(fd, header, 5) < 0) return -1;
    if (len && write_all(fd, payload, len) < 0) return -1;
    return 0;
}*/
//===================================================================
// ارسال خطا
int send_error(int fd, const char *code, const char *message) {
    char error_json[256];
    snprintf(error_json, sizeof(error_json),
             "{\"code\":\"%s\",\"message\":\"%s\"}", code, message);
    return send_frame(fd, OP_ERROR, error_json, strlen(error_json));
}

// تبدیل هش به هگزادسیمال
void hash_to_hex(const uint8_t *hash, char *hex) {
    for (int i = 0; i < HASH_LENGTH; i++) {
        sprintf(hex + i * 2, "%02x", hash[i]);
    }
    hex[64] = '\0';
}

// تبدیل هگزادسیمال به هش باینری
int hex_to_hash(const char *hex, uint8_t *hash) {
    for (int i = 0; i < HASH_LENGTH; i++) {
        if (sscanf(hex + i * 2, "%2hhx", &hash[i]) != 1) {
            return -1;
        }
    }
    return 0;
}

// محاسبه هش SHA256
void compute_hash(const uint8_t *data, size_t len, uint8_t *hash) {
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, data, len);
    SHA256_Final(hash, &sha256);
}

// کدگذاری base32
char *base32_encode(const unsigned char *data, int len) {
    const char *base32_chars = "abcdefghijklmnopqrstuvwxyz234567";
    int output_len = (len * 8 + 4) / 5;
    char *encoded = malloc(output_len + 1);
    if (!encoded) return NULL;

    int bit_buffer = 0;
    int bit_count = 0;
    int out_index = 0;

    for (int i = 0; i < len; i++) {
        bit_buffer = (bit_buffer << 8) | data[i];
        bit_count += 8;
        while (bit_count >= 5) {
            int index = (bit_buffer >> (bit_count - 5)) & 0x1F;
            encoded[out_index++] = base32_chars[index];
            bit_count -= 5;
        }
    }

    if (bit_count > 0) {
        int index = (bit_buffer << (5 - bit_count)) & 0x1F;
        encoded[out_index++] = base32_chars[index];
    }

    encoded[out_index] = '\0';
    return encoded;
}

// تولید CID
char *generate_cid(const char *manifest_json) {
    // محاسبه هش مانیفست
    uint8_t manifest_hash[HASH_LENGTH];
    compute_hash((const uint8_t*)manifest_json, strlen(manifest_json), manifest_hash);
   
    // ساخت multihash: [کد-الگوریتم, طول-هش, هش]
    uint8_t multihash[2 + HASH_LENGTH];
    multihash[0] = 0x12;  // کد SHA256 در multihash (0x12 = 18)
    multihash[1] = HASH_LENGTH;
    memcpy(multihash + 2, manifest_hash, HASH_LENGTH);
   
    // ساخت multicodec + multihash
    uint8_t multicodec_data[2 + 2 + HASH_LENGTH]; // 36 بایت
    unsigned int dag_json_code = 0x0129; // کد dag-json (297)
   
    // کدگذاری Varint برای 297
    multicodec_data[0] = (dag_json_code >> 7) | 0x80; // 0x82
    multicodec_data[1] = (dag_json_code & 0x7F);      // 0x29
    
    // کپی multihash
    memcpy(multicodec_data + 2, multihash, 2 + HASH_LENGTH);
   
    // کدگذاری Base32
    char *base32 = base32_encode(multicodec_data, 2 + 2 + HASH_LENGTH);
    if (!base32) return NULL;
   
    char *cid = malloc(strlen(base32) + 2);
    sprintf(cid, "b%s", base32);
    free(base32);
   
    return cid;
}

// تابع هش برای CID
static unsigned int hash_cid(const char *cid) {
    unsigned int hash = 5381;
    int c;
    while ((c = *cid++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

// ==================== مدیریت فایل‌های بلاک ====================

// ساخت مسیر فایل بلاک بر اساس هش
char *build_block_path(const char *hash_hex) {
    char *path = malloc(512);
    if (!path) return NULL;
    
    snprintf(path, 512, "%s/%.2s/%.2s/%s", 
             BLOCKS_DIR, hash_hex, hash_hex + 2, hash_hex);
    return path;
}

// ذخیره بلاک در فایل
int store_block(const char *hash_hex, const uint8_t *data, size_t size) {
    char *path = build_block_path(hash_hex);
    if (!path) return -1;
    
    // ایجاد دایرکتوری‌ها
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%.2s/%.2s", 
             BLOCKS_DIR, hash_hex, hash_hex + 2);
    ensure_dir(dir_path);
    
    // نوشتن فایل با روش اتمیک
    char temp_path[512];
    snprintf(temp_path, sizeof(temp_path), "%s.tmp", path);
    
    int fd = open(temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        free(path);
        return -1;
    }
    
    if (write_all(fd, data, size) < 0) {
        close(fd);
        unlink(temp_path);
        free(path);
        return -1;
    }
    
    fsync(fd);
    close(fd);
    
    // تغییر نام اتمیک
    if (rename(temp_path, path) < 0) {
        unlink(temp_path);
        free(path);
        return -1;
    }
    
    free(path);
    return 0;
}

// خواندن بلاک از فایل
uint8_t *read_block(const char *hash_hex, size_t *size) {
    char *path = build_block_path(hash_hex);
    if (!path) return NULL;
    
    int fd = open(path, O_RDONLY);
    free(path);
    if (fd < 0) return NULL;
    
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return NULL;
    }
    
    *size = st.st_size;
    uint8_t *data = malloc(*size);
    if (!data) {
        close(fd);
        return NULL;
    }
    
    ssize_t bytes_read = read(fd, data, *size);
    close(fd);
    
    if (bytes_read != *size) {
        free(data);
        return NULL;
    }
    
    return data;
}

// ==================== مدیریت مانیفست‌ها ====================

// پارس کردن JSON مانیفست
int parse_manifest(const char *json_str, manifest_info_t *manifest) {
    // این یک پارسر ساده JSON است - برای سادگی فرض می‌کنیم JSON معتبر است
    // در عمل بهتر است از یک کتابخانه JSON مانند jansson استفاده شود
    
    char filename[256] = {0};
    long long total_size = 0;
    int chunk_count = 0;
    int chunk_size = 0;
    
    // استخراج مقادیر پایه
    const char *pos = json_str;
    
    // استخراج filename
    pos = strstr(pos, "\"filename\"");
    if (pos) {
        pos = strchr(pos, ':');
        pos = strchr(pos, '\"');
        if (pos) {
            pos++;
            const char *end = strchr(pos, '\"');
            if (end) {
                int len = end - pos;
                if (len > 255) len = 255;
                strncpy(filename, pos, len);
                filename[len] = '\0';
            }
        }
    }
    
    // استخراج total_size
    pos = strstr(json_str, "\"total_size\"");
    if (pos) {
        sscanf(pos, "\"total_size\": %lld,", &total_size);
    }
    
    // استخراج chunk_size
    pos = strstr(json_str, "\"chunk_size\"");
    if (pos) {
        sscanf(pos, "\"chunk_size\": %d,", &chunk_size);
    }
    
    // شمارش تعداد chunks
    pos = json_str;
    while ((pos = strstr(pos, "\"index\"")) != NULL) {
        chunk_count++;
        pos += 6;
    }
    
    // تخصیص حافظه
    manifest->filename = strdup(filename);
    manifest->total_size = total_size;
    manifest->chunk_count = chunk_count;
    manifest->chunks = calloc(chunk_count, sizeof(file_part_t));
    
    // استخراج اطلاعات chunks
    pos = json_str;
    int index = 0;
    
    while ((pos = strstr(pos, "\"index\"")) != NULL && index < chunk_count) {
        // استخراج index
        int chunk_index;
        sscanf(pos, "\"index\": %d,", &chunk_index);
        
        // حرکت به hash
        pos = strstr(pos, "\"hash\"");
        if (!pos) break;
        
        // استخراج hash
        pos = strchr(pos, '\"');
        if (!pos) break;
        pos++;
        
        const char *hash_start = pos;
        const char *hash_end = strchr(pos, '\"');
        if (!hash_end) break;
        
        char hash_hex[65];
        int hash_len = hash_end - hash_start;
        if (hash_len > 64) hash_len = 64;
        strncpy(hash_hex, hash_start, hash_len);
        hash_hex[hash_len] = '\0';
        
        // استخراج size
        const char *size_pos = strstr(pos, "\"size\"");
        int chunk_sz = 0;
        if (size_pos) {
            sscanf(size_pos, "\"size\": %d,", &chunk_sz);
        }
        
        // ذخیره اطلاعات
        manifest->chunks[index].index = chunk_index;
        manifest->chunks[index].size = chunk_sz;
        hex_to_hash(hash_hex, manifest->chunks[index].hash);
        
        char *path = build_block_path(hash_hex);
        manifest->chunks[index].file_path = path;
        
        index++;
        pos = hash_end;
    }
    
    return 0;
}

// آزادسازی حافظه مانیفست
void free_manifest_info(manifest_info_t *manifest) {
    if (!manifest) return;
    
    free(manifest->filename);
    if (manifest->chunks) {
        for (int i = 0; i < manifest->chunk_count; i++) {
            free(manifest->chunks[i].file_path);
        }
        free(manifest->chunks);
    }
    free(manifest);
}

// ذخیره مانیفست
int store_manifest(const char *cid, const char *manifest_json) {
    ensure_dir(MANIFESTS_DIR);
    
    char manifest_path[512];
    char temp_path[512];
    
    snprintf(manifest_path, sizeof(manifest_path), "%s/%s.json", MANIFESTS_DIR, cid);
    snprintf(temp_path, sizeof(temp_path), "%s.tmp", manifest_path);
    
    // نوشتن در فایل موقت
    int fd = open(temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
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
        unlink(temp_path);
        return -1;
    }
    
    return 0;
}

// خواندن مانیفست
char *read_manifest(const char *cid) {
    char manifest_path[512];
    snprintf(manifest_path, sizeof(manifest_path), "%s/%s.json", MANIFESTS_DIR, cid);
    
    FILE *file = fopen(manifest_path, "r");
    if (!file) {
        return NULL;
    }
    
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    fseek(file, 0, SEEK_SET);
    
    if (file_size > MAX_MANIFEST_SIZE) {
        fclose(file);
        return NULL;
    }
    
    char *manifest_json = malloc(file_size + 1);
    if (!manifest_json) {
        fclose(file);
        return NULL;
    }
    
    size_t bytes_read = fread(manifest_json, 1, file_size, file);
    manifest_json[bytes_read] = '\0';
    
    fclose(file);
    return manifest_json;
}

// ==================== مدیریت جدول بلاک‌ها ====================

// پیدا کردن بلاک در جدول
chunk_entry_t *find_chunk(const char *hash_hex) {
    pthread_mutex_lock(&chunk_table_lock);
    
    for (int i = 0; i < chunk_table_size; i++) {
        if (chunk_table[i] && strcmp(chunk_table[i]->hash_hex, hash_hex) == 0) {
            pthread_mutex_unlock(&chunk_table_lock);
            return chunk_table[i];
        }
    }
    
    pthread_mutex_unlock(&chunk_table_lock);
    return NULL;
}

// اضافه کردن بلاک به جدول
chunk_entry_t *add_chunk(const char *hash_hex, const char *file_path) {
    chunk_entry_t *chunk = malloc(sizeof(chunk_entry_t));
    if (!chunk) return NULL;
    
    strncpy(chunk->hash_hex, hash_hex, 64);
    chunk->hash_hex[64] = '\0';
    chunk->file_path = strdup(file_path);
    chunk->ref_count = 1;
    chunk->last_access = time(NULL);
    chunk->marked_for_deletion = 0;
    pthread_mutex_init(&chunk->lock, NULL);
    
    pthread_mutex_lock(&chunk_table_lock);
    
    chunk_table_size++;
    chunk_entry_t **new_table = realloc(chunk_table, 
                                       chunk_table_size * sizeof(chunk_entry_t*));
    if (!new_table) {
        free(chunk->file_path);
        free(chunk);
        pthread_mutex_unlock(&chunk_table_lock);
        return NULL;
    }
    
    chunk_table = new_table;
    chunk_table[chunk_table_size - 1] = chunk;
    
    pthread_mutex_unlock(&chunk_table_lock);
    return chunk;
}

// افزایش شمارنده ارجاع
int increment_ref_count(const char *hash_hex) {
    chunk_entry_t *chunk = find_chunk(hash_hex);
    if (!chunk) return -1;
    
    pthread_mutex_lock(&chunk->lock);
    chunk->ref_count++;
    chunk->last_access = time(NULL);
    pthread_mutex_unlock(&chunk->lock);
    
    return 0;
}

// کاهش شمارنده ارجاع
int decrement_ref_count(const char *hash_hex) {
    chunk_entry_t *chunk = find_chunk(hash_hex);
    if (!chunk) return -1;
    
    pthread_mutex_lock(&chunk->lock);
    chunk->ref_count--;
    chunk->last_access = time(NULL);
    
    if (chunk->ref_count <= 0) {
        chunk->marked_for_deletion = 1;
        // حذف فایل
        unlink(chunk->file_path);
    }
    
    pthread_mutex_unlock(&chunk->lock);
    return 0;
}

// ==================== مدیریت جدول مانیفست‌ها ====================

// پیدا کردن مانیفست در جدول
manifest_entry_t *find_manifest_entry(const char *cid) {
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

// اضافه کردن مانیفست به جدول
manifest_entry_t *add_manifest_entry(const char *cid, manifest_info_t *manifest_info) {
    manifest_entry_t *entry = malloc(sizeof(manifest_entry_t));
    if (!entry) return NULL;
    
    strncpy(entry->cid, cid, 127);
    entry->cid[127] = '\0';
    entry->manifest_info = manifest_info;
    entry->ref_count = 1;
    entry->last_access = time(NULL);
    entry->marked_for_deletion = 0;
    pthread_mutex_init(&entry->lock, NULL);
    
    pthread_mutex_lock(&manifest_table_lock);
    
    manifest_table_size++;
    manifest_entry_t **new_table = realloc(manifest_table, 
                                          manifest_table_size * sizeof(manifest_entry_t*));
    if (!new_table) {
        free(entry);
        pthread_mutex_unlock(&manifest_table_lock);
        return NULL;
    }
    
    manifest_table = new_table;
    manifest_table[manifest_table_size - 1] = entry;
    
    pthread_mutex_unlock(&manifest_table_lock);
    return entry;
}

// ==================== مدیریت قفل مانیفست ====================

// مقداردهی اولیه سیستم قفل‌ها
int manifest_lock_init(void) {
    memset(&g_lock_table, 0, sizeof(g_lock_table));
    if (pthread_mutex_init(&g_lock_table.table_lock, NULL) != 0) {
        return -1;
    }
    for (int i = 0; i < LOCK_TABLE_SIZE; i++) {
        g_lock_table.locks[i] = NULL;
    }
    return 0;
}

// تخریب سیستم قفل‌ها
int manifest_lock_destroy(void) {
    for (int i = 0; i < LOCK_TABLE_SIZE; i++) {
        manifest_lock_entry_t *entry = g_lock_table.locks[i];
        while (entry != NULL) {
            manifest_lock_entry_t *next = entry->next;
            pthread_rwlock_destroy(&entry->rwlock);
            free(entry);
            entry = next;
        }
        g_lock_table.locks[i] = NULL;
    }
    pthread_mutex_destroy(&g_lock_table.table_lock);
    return 0;
}

// پیدا کردن یا ایجاد entry برای یک CID
static manifest_lock_entry_t* get_or_create_lock_entry(const char *cid) {
    unsigned int bucket = hash_cid(cid) % LOCK_TABLE_SIZE;
    
    pthread_mutex_lock(&g_lock_table.table_lock);
    
    manifest_lock_entry_t *entry = g_lock_table.locks[bucket];
    manifest_lock_entry_t **prev_next = &g_lock_table.locks[bucket];
    
    while (entry != NULL) {
        if (strcmp(entry->cid, cid) == 0) {
            entry->ref_count++;
            pthread_mutex_unlock(&g_lock_table.table_lock);
            return entry;
        }
        prev_next = &entry->next;
        entry = entry->next;
    }
    
    manifest_lock_entry_t *new_entry = malloc(sizeof(manifest_lock_entry_t));
    if (new_entry == NULL) {
        pthread_mutex_unlock(&g_lock_table.table_lock);
        return NULL;
    }
    
    strncpy(new_entry->cid, cid, CID_MAX_LEN - 1);
    new_entry->cid[CID_MAX_LEN - 1] = '\0';
    
    if (pthread_rwlock_init(&new_entry->rwlock, NULL) != 0) {
        free(new_entry);
        pthread_mutex_unlock(&g_lock_table.table_lock);
        return NULL;
    }
    
    new_entry->ref_count = 1;
    new_entry->next = NULL;
    *prev_next = new_entry;
    
    pthread_mutex_unlock(&g_lock_table.table_lock);
    return new_entry;
}

// کاهش ref_count
static int decrement_lock_ref_count(const char *cid) {
    unsigned int bucket = hash_cid(cid) % LOCK_TABLE_SIZE;
    
    pthread_mutex_lock(&g_lock_table.table_lock);
    
    manifest_lock_entry_t *entry = g_lock_table.locks[bucket];
    manifest_lock_entry_t *prev = NULL;
    
    while (entry != NULL && strcmp(entry->cid, cid) != 0) {
        prev = entry;
        entry = entry->next;
    }
    
    if (entry == NULL) {
        pthread_mutex_unlock(&g_lock_table.table_lock);
        return -1;
    }
    
    entry->ref_count--;
    
    if (entry->ref_count == 0) {
        if (prev == NULL) {
            g_lock_table.locks[bucket] = entry->next;
        } else {
            prev->next = entry->next;
        }
        
        pthread_mutex_unlock(&g_lock_table.table_lock);
        pthread_rwlock_destroy(&entry->rwlock);
        free(entry);
    } else {
        pthread_mutex_unlock(&g_lock_table.table_lock);
    }
    
    return 0;
}

// گرفتن قفل خواندن
int manifest_read_lock(const char *cid) {
    manifest_lock_entry_t *entry = get_or_create_lock_entry(cid);
    if (entry == NULL) {
        return LOCK_NO_MEMORY;
    }
    
    if (pthread_rwlock_rdlock(&entry->rwlock) != 0) {
        pthread_mutex_lock(&g_lock_table.table_lock);
        entry->ref_count--;
        pthread_mutex_unlock(&g_lock_table.table_lock);
        return LOCK_FAILED;
    }
    
    return LOCK_SUCCESS;
}

// گرفتن قفل نوشتن
int manifest_write_lock(const char *cid) {
    manifest_lock_entry_t *entry = get_or_create_lock_entry(cid);
    if (entry == NULL) {
        return LOCK_NO_MEMORY;
    }
    
    if (pthread_rwlock_wrlock(&entry->rwlock) != 0) {
        pthread_mutex_lock(&g_lock_table.table_lock);
        entry->ref_count--;
        pthread_mutex_unlock(&g_lock_table.table_lock);
        return LOCK_FAILED;
    }
    
    return LOCK_SUCCESS;
}

// آزاد کردن قفل
int manifest_unlock(const char *cid, int is_writer) {
    unsigned int bucket = hash_cid(cid) % LOCK_TABLE_SIZE;
    
    pthread_mutex_lock(&g_lock_table.table_lock);
    
    manifest_lock_entry_t *entry = g_lock_table.locks[bucket];
    while (entry != NULL && strcmp(entry->cid, cid) != 0) {
        entry = entry->next;
    }
    
    if (entry == NULL) {
        pthread_mutex_unlock(&g_lock_table.table_lock);
        return -1;
    }
    
    pthread_mutex_unlock(&g_lock_table.table_lock);
    
    if (pthread_rwlock_unlock(&entry->rwlock) != 0) {
        return -1;
    }
    
    return decrement_lock_ref_count(cid);
}

// ==================== Thread Pool ====================

// تابع کارگر
static void* worker_thread(void *arg) {
    thread_pool_t *pool = (thread_pool_t*)arg;
    
    while (1) {
        task_t *task = NULL;
        
        pthread_mutex_lock(&pool->queue_lock);
        
        while (pool->queue_size == 0 && !pool->shutdown) {
            pthread_cond_wait(&pool->queue_not_empty, &pool->queue_lock);
        }
        
        if (pool->shutdown && pool->queue_size == 0) {
            pthread_mutex_unlock(&pool->queue_lock);
            break;
        }
        
        task = pool->task_queue_head;
        if (task) {
            pool->task_queue_head = task->next;
            if (pool->task_queue_head == NULL) {
                pool->task_queue_tail = NULL;
            }
            pool->queue_size--;
            pool->active_tasks++;
            pthread_cond_signal(&pool->queue_not_full);
        }
        
        pthread_mutex_unlock(&pool->queue_lock);
        
        if (task) {
            // اجرای وظیفه بر اساس نوع
            int result = 0;
            
            switch (task->type) {
                case TASK_TYPE_UPLOAD_CHUNK: {
                    // پردازش چانک آپلود
                    upload_state_t *state = task->data.upload.upload_state;
                    int chunk_index = task->data.upload.chunk_index;
                    uint8_t *data = task->data.upload.data;
                    size_t data_size = task->data.upload.data_size;
                    
                    // محاسبه هش
                    uint8_t hash[HASH_LENGTH];
                    compute_hash(data, data_size, hash);
                    
                    char hash_hex[65];
                    hash_to_hex(hash, hash_hex);
                    
                    // ذخیره بلاک
                    if (store_block(hash_hex, data, data_size) == 0) {
                        // اضافه کردن به جدول بلاک‌ها
                        char *path = build_block_path(hash_hex);
                        chunk_entry_t *chunk = find_chunk(hash_hex);
                        if (!chunk) {
                            chunk = add_chunk(hash_hex, path);
                        } else {
                            increment_ref_count(hash_hex);
                        }
                        free(path);
                        
                        // ذخیره اطلاعات در وضعیت آپلود
                        pthread_mutex_lock(&state->lock);
                        if (chunk_index < state->chunk_count) {
                            state->chunks[chunk_index].index = chunk_index;
                            state->chunks[chunk_index].size = data_size;
                            memcpy(state->chunks[chunk_index].hash, hash, HASH_LENGTH);
                            state->chunks[chunk_index].file_path = build_block_path(hash_hex);
                        }
                        state->chunks_processed++;
                        
                        if (state->chunks_processed == state->chunk_count) {
                            pthread_cond_signal(&state->all_chunks_done);
                        }
                        pthread_mutex_unlock(&state->lock);
                        
                        printf("[THREAD] Processed upload chunk %d, hash: %s\n", 
                               chunk_index, hash_hex);
                    } else {
                        result = -1;
                    }
                    break;
                }
                
                case TASK_TYPE_DOWNLOAD_CHUNK: {
                    // پردازش چانک دانلود
                    download_state_t *state = task->data.download.download_state;
                    int chunk_index = task->data.download.chunk_index;
                    char *chunk_hash = task->data.download.chunk_hash;
                    char *chunk_path = task->data.download.chunk_path;
                    size_t expected_size = task->data.download.expected_size;
                    
                    // خواندن بلاک
                    size_t actual_size;
                    uint8_t *data = read_block(chunk_hash, &actual_size);
                    
                    if (data && actual_size == expected_size) {
                        // تأیید هش
                        uint8_t computed_hash[HASH_LENGTH];
                        compute_hash(data, actual_size, computed_hash);
                        
                        char computed_hash_hex[65];
                        hash_to_hex(computed_hash, computed_hash_hex);
                        
                        if (strcmp(computed_hash_hex, chunk_hash) == 0) {
                            // ذخیره در وضعیت دانلود
                            pthread_mutex_lock(&state->lock);
                            state->chunk_data[chunk_index] = data;
                            state->chunk_sizes[chunk_index] = actual_size;
                            state->chunk_ready[chunk_index] = 1;
                            state->chunks_ready_count++;
                            
                            if (chunk_index == state->next_expected_index) {
                                pthread_cond_signal(&state->chunk_ready_cond);
                            }
                            pthread_mutex_unlock(&state->lock);
                            
                            printf("[THREAD] Processed download chunk %d\n", chunk_index);
                        } else {
                            free(data);
                            result = -1;
                        }
                    } else {
                        if (data) free(data);
                        result = -1;
                    }
                    break;
                }
                
                case TASK_TYPE_COMPUTE_HASH: {
                    // محاسبه هش
                    compute_hash(task->data.hash.data, 
                               task->data.hash.data_size,
                               task->data.hash.hash);
                    break;
                }
                
                case TASK_TYPE_WRITE_FILE: {
                    // نوشتن فایل
                    int fd = open(task->data.write_file.file_path, 
                                 O_WRONLY | O_CREAT | O_TRUNC, 0644);
                    if (fd >= 0) {
                        write_all(fd, task->data.write_file.data, 
                                 task->data.write_file.data_size);
                        close(fd);
                    }
                    break;
                }
                
                case TASK_TYPE_DELETE_FILE: {
                    // حذف فایل
                    unlink(task->data.delete_file.file_path);
                    break;
                }
            }
            
            // فراخوانی callback
            if (task->callback) {
                task->callback(task->callback_arg, result);
            }
            
            pthread_mutex_lock(&pool->queue_lock);
            pool->active_tasks--;
            if (result == 0) {
                pool->tasks_completed++;
            } else {
                pool->tasks_failed++;
            }
            
            if (pool->shutdown && pool->queue_size == 0 && pool->active_tasks == 0) {
                pthread_cond_signal(&pool->queue_empty);
            }
            pthread_mutex_unlock(&pool->queue_lock);
            
            // آزادسازی
            if (task->type == TASK_TYPE_UPLOAD_CHUNK) {
                free(task->data.upload.data);
            } else if (task->type == TASK_TYPE_WRITE_FILE) {
                free(task->data.write_file.data);
                free(task->data.write_file.file_path);
            } else if (task->type == TASK_TYPE_DELETE_FILE) {
                free(task->data.delete_file.file_path);
            }
            free(task);
        }
    }
    
    return NULL;
}

// مقداردهی اولیه Thread Pool
int thread_pool_init(thread_pool_t *pool, int max_workers, int max_queue_size) {
    pool->workers = malloc(max_workers * sizeof(pthread_t));
    if (!pool->workers) return -1;
    
    pool->worker_count = max_workers;
    pool->max_workers = max_workers;
    pool->max_queue_size = max_queue_size;
    pool->shutdown = 0;
    pool->queue_size = 0;
    pool->active_tasks = 0;
    pool->tasks_completed = 0;
    pool->tasks_failed = 0;
    pool->task_queue_head = NULL;
    pool->task_queue_tail = NULL;
    atomic_init(&pool->next_task_id, 1);
    
    pthread_mutex_init(&pool->queue_lock, NULL);
    pthread_cond_init(&pool->queue_not_empty, NULL);
    pthread_cond_init(&pool->queue_not_full, NULL);
    pthread_cond_init(&pool->queue_empty, NULL);
    
    for (int i = 0; i < max_workers; i++) {
        if (pthread_create(&pool->workers[i], NULL, worker_thread, pool) != 0) {
            for (int j = 0; j < i; j++) {
                pthread_cancel(pool->workers[j]);
            }
            free(pool->workers);
            return -1;
        }
    }
    
    return 0;
}

// ارسال وظیفه به Thread Pool
int thread_pool_submit_task(thread_pool_t *pool, task_t *task) {
    if (!task || pool->shutdown) return -1;
    
    pthread_mutex_lock(&pool->queue_lock);
    
    while (pool->queue_size >= pool->max_queue_size && !pool->shutdown) {
        pthread_cond_wait(&pool->queue_not_full, &pool->queue_lock);
    }
    
    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->queue_lock);
        return -1;
    }
    
    task->next = NULL;
    if (pool->task_queue_tail) {
        pool->task_queue_tail->next = task;
        pool->task_queue_tail = task;
    } else {
        pool->task_queue_head = task;
        pool->task_queue_tail = task;
    }
    
    pool->queue_size++;
    pthread_cond_signal(&pool->queue_not_empty);
    pthread_mutex_unlock(&pool->queue_lock);
    
    return 0;
}

// خاموش کردن Thread Pool
void thread_pool_shutdown(thread_pool_t *pool) {
    pthread_mutex_lock(&pool->queue_lock);
    pool->shutdown = 1;
    pthread_cond_broadcast(&pool->queue_not_empty);
    pthread_cond_broadcast(&pool->queue_not_full);
    pthread_mutex_unlock(&pool->queue_lock);
    
    for (int i = 0; i < pool->worker_count; i++) {
        pthread_join(pool->workers[i], NULL);
    }
    
    free(pool->workers);
    
    pthread_mutex_destroy(&pool->queue_lock);
    pthread_cond_destroy(&pool->queue_not_empty);
    pthread_cond_destroy(&pool->queue_not_full);
    pthread_cond_destroy(&pool->queue_empty);
}

// ایجاد وظیفه آپلود چانک
task_t *create_upload_chunk_task(upload_state_t *state, int chunk_index,
                                 uint8_t *data, size_t data_size,
                                 void (*callback)(void*, int), void *cb_arg) {
    task_t *task = calloc(1, sizeof(task_t));
    if (!task) return NULL;
    
    task->type = TASK_TYPE_UPLOAD_CHUNK;
    task->data.upload.upload_state = state;
    task->data.upload.chunk_index = chunk_index;
    task->data.upload.data = malloc(data_size);
    if (!task->data.upload.data) {
        free(task);
        return NULL;
    }
    memcpy(task->data.upload.data, data, data_size);
    task->data.upload.data_size = data_size;
    task->callback = callback;
    task->callback_arg = cb_arg;
    task->task_id = atomic_fetch_add(&g_thread_pool.next_task_id, 1);
    
    return task;
}

// ایجاد وظیفه دانلود چانک
task_t *create_download_chunk_task(download_state_t *state, int chunk_index,
                                   const char *chunk_hash, const char *chunk_path,
                                   size_t expected_size,
                                   void (*callback)(void*, int), void *cb_arg) {
    task_t *task = calloc(1, sizeof(task_t));
    if (!task) return NULL;
    
    task->type = TASK_TYPE_DOWNLOAD_CHUNK;
    task->data.download.download_state = state;
    task->data.download.chunk_index = chunk_index;
    // کپی کردن رشته‌ها به جای اشاره مستقیم
    if (chunk_hash) {
        strncpy(task->data.download.chunk_hash, chunk_hash, 64);
        task->data.download.chunk_hash[64] = '\0';
    }
    //else 
    if (chunk_path) {
        strncpy(task->data.download.chunk_path, chunk_path, 511);
        task->data.download.chunk_path[511] = '\0';
    }
    //else task->data.download.chunk_path = NULL;
    /*strncpy(task->data.download.chunk_hash, chunk_hash, 64);
    task->data.download.chunk_hash[64] = '\0';
    strncpy(task->data.download.chunk_path, chunk_path, 511);
    task->data.download.chunk_path[511] = '\0';*/
    task->data.download.expected_size = expected_size;
    task->callback = callback;
    task->callback_arg = cb_arg;
    task->task_id = atomic_fetch_add(&g_thread_pool.next_task_id, 1);
    
    return task;
}

// ==================== توابع آپلود ====================

// پردازش شروع آپلود
upload_state_t* handle_upload_start(int client_fd, const uint8_t *payload, uint32_t len) {
    printf("[ENGINE] UPLOAD_START received\n");
    
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
    
    memset(state, 0, sizeof(upload_state_t));
    strncpy(state->filename, token, 255);
    
    token = strtok(NULL, "|");
    if (token) state->total_size = atoll(token);
    
    token = strtok(NULL, "|");
    if (token) state->chunk_size = atoi(token);
    else state->chunk_size = CHUNK_SIZE;
    
    token = strtok(NULL, "|");
    if (token) strncpy(state->hash_algo, token, 31);
    else strcpy(state->hash_algo, HASH_ALGO);
    
    // محاسبه تعداد چانک‌ها
    state->chunk_count = (state->total_size + state->chunk_size - 1) / state->chunk_size;
    state->chunks = calloc(state->chunk_count, sizeof(state->chunks[0]));
    state->chunks_received = 0;
    state->chunks_processed = 0;
    state->client_fd = client_fd;
    
    pthread_mutex_init(&state->lock, NULL);
    pthread_cond_init(&state->all_chunks_done, NULL);
    
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
    
    if (len < 4) {
        send_error(client_fd, E_PROTO, "Invalid chunk format");
        return;
    }
    
    // استخراج index از 4 بایت اول
    uint32_t index;
    memcpy(&index, payload, 4);
    index = ntohl(index);
    
    // بررسی index
    if (index >= state->chunk_count) {
        send_error(client_fd, E_PROTO, "Invalid chunk index");
        return;
    }
    
    // داده‌های چانک
    uint8_t *data = (uint8_t*)(payload + 4);
    size_t data_size = len - 4;
    
    // ایجاد callback برای اتمام پردازش
    void upload_chunk_callback(void *arg, int result) {
        // این callback توسط Thread Pool فراخوانی می‌شود
        // در این نسخه ساده کاری انجام نمی‌دهیم
    }
    
    // ایجاد و ارسال وظیفه به Thread Pool
    task_t *task = create_upload_chunk_task(state, index, data, data_size,
                                           upload_chunk_callback, NULL);
    if (!task) {
        send_error(client_fd, E_BUSY, "Failed to create task");
        return;
    }
    
    if (thread_pool_submit_task(&g_thread_pool, task) < 0) {
        free(task->data.upload.data);
        free(task);
        send_error(client_fd, E_BUSY, "Thread pool is busy");
        return;
    }
    
    state->chunks_received++;
    printf("[ENGINE] Chunk %d queued for processing (%d/%d)\n",
           index, state->chunks_received, state->chunk_count);
}

// ایجاد JSON مانیفست
char *create_manifest_json(upload_state_t *state) {
    // محاسبه اندازه مورد نیاز
    int json_size = 1024; // اندازه پایه
    for (int i = 0; i < state->chunk_count; i++) {
        json_size += 256; // برای هر چانک
    }
    
    char *manifest_json = malloc(json_size);
    if (!manifest_json) return NULL;
    
    char *ptr = manifest_json;
    int written = snprintf(ptr, json_size,
                          "{\n"
                          "  \"version\": 1,\n"
                          "  \"hash_algo\": \"%s\",\n"
                          "  \"chunk_size\": %d,\n"
                          "  \"total_size\": %lld,\n"
                          "  \"filename\": \"%s\",\n"
                          "  \"chunks\": [\n",
                          state->hash_algo, state->chunk_size,
                          state->total_size, state->filename);
    
    ptr += written;
    
    // افزودن اطلاعات چانک‌ها
    for (int i = 0; i < state->chunk_count; i++) {
        char hash_hex[65];
        hash_to_hex(state->chunks[i].hash, hash_hex);
        
        written = snprintf(ptr, (manifest_json + json_size) - ptr,
                          "    {\"index\": %d, \"size\": %d, \"hash\": \"%s\"}%s\n",
                          state->chunks[i].index, state->chunks[i].size, hash_hex,
                          (i == state->chunk_count - 1) ? "" : ",");
        
        ptr += written;
    }
    
    // پایان JSON
    strcpy(ptr, "  ]\n}\n");
    
    return manifest_json;
}

// پردازش پایان آپلود
void handle_upload_finish(int client_fd, upload_state_t *state) {
    if (!state) {
        send_error(client_fd, E_PROTO, "No upload started");
        return;
    }
    
    printf("[ENGINE] UPLOAD_FINISH received\n");
    
    // منتظر ماندن برای اتمام تمام چانک‌ها
    pthread_mutex_lock(&state->lock);
    while (state->chunks_processed < state->chunk_count) {
        pthread_cond_wait(&state->all_chunks_done, &state->lock);
    }
    
    // ایجاد مانیفست
    char *manifest_json = create_manifest_json(state);
    if (!manifest_json) {
        pthread_mutex_unlock(&state->lock);
        send_error(client_fd, E_BUSY, "Failed to create manifest");
        return;
    }
    
    // تولید CID
    char *cid = generate_cid(manifest_json);
    if (!cid) {
        free(manifest_json);
        pthread_mutex_unlock(&state->lock);
        send_error(client_fd, E_BUSY, "Failed to generate CID");
        return;
    }
    
    // ذخیره مانیفست
    if (store_manifest(cid, manifest_json) < 0) {
        free(cid);
        free(manifest_json);
        pthread_mutex_unlock(&state->lock);
        send_error(client_fd, E_BUSY, "Failed to store manifest");
        return;
    }
    
    // پارس کردن مانیفست و ذخیره در جدول
    manifest_info_t *manifest_info = malloc(sizeof(manifest_info_t));
    if (parse_manifest(manifest_json, manifest_info) < 0) {
        free(cid);
        free(manifest_json);
        free(manifest_info);
        pthread_mutex_unlock(&state->lock);
        send_error(client_fd, E_BUSY, "Failed to parse manifest");
        return;
    }
    
    // ذخیره در جدول مانیفست‌ها
    add_manifest_entry(cid, manifest_info);
    
    // ارسال پاسخ
    send_frame(client_fd, OP_UPLOAD_DONE, cid, strlen(cid));
    
    printf("[ENGINE] Upload completed: CID=%s, size=%lld, chunks=%d\n",
           cid, state->total_size, state->chunk_count);
    
    // پاکسازی
    free(cid);
    free(manifest_json);
    pthread_mutex_unlock(&state->lock);
    
    // پاکسازی وضعیت آپلود
    for (int i = 0; i < state->chunk_count; i++) {
        free(state->chunks[i].file_path);
    }
    free(state->chunks);
    pthread_mutex_destroy(&state->lock);
    pthread_cond_destroy(&state->all_chunks_done);
    free(state);
}

// ==================== توابع دانلود ====================

// شروع دانلود
void download_start(int client_fd, const char *cid,download_state_t* state) {
    //printf("[ENGINE] DOWNLOAD_START: cid=\"%s\"\n", cid);
    /*
    // ساختار وضعیت دانلود
typedef struct download_state {
    
    uint8_t **chunk_data;
    size_t *chunk_sizes;
    int *chunk_ready;
        
} download_state_t;
    */
    // اعتبارسنجی CID
    //printf("hi?");
    if (strlen(cid) < 10) {
        //printf("trying10");
        send_error(client_fd, E_BAD_CID, "Invalid CID format");
        return ;
    }
    //printf("trying lock");
    // گرفتن قفل خواندن برای مانیفست
    if (manifest_read_lock(cid) != LOCK_SUCCESS) {
        send_error(client_fd, E_BUSY, "System busy");
        return ;
    }
    //printf("0");
    manifest_entry_t *manifest_entry = find_manifest_entry(cid);
    //printf("1");
    manifest_info_t * manifest_info = NULL;
    if(manifest_entry){
        manifest_info = manifest_entry->manifest_info;
        free(manifest_entry);
    }
    if (!manifest_info){
        // خواندن مانیفست
        char *manifest_json = read_manifest(cid);
        if (!manifest_json) {
            manifest_unlock(cid, 0);
            send_error(client_fd, E_NOT_FOUND, "Manifest not found");
            return ;
        }
        
        // پارس کردن مانیفست
        manifest_info = malloc(sizeof(manifest_info_t));
        
        if (parse_manifest(manifest_json, manifest_info) < 0) {
            free(manifest_json);
            free(manifest_info);
            manifest_unlock(cid, 0);
            send_error(client_fd, E_BAD_CID, "Failed to parse manifest");
            return ;
        }
        //printf("parsed");
        free(manifest_json);
        add_manifest_entry(cid,manifest_info);
    }
    

    // ایجاد وضعیت دانلود
    state = malloc(sizeof(download_state_t));
    if (!state) {
        
        free_manifest_info(manifest_info);
        manifest_unlock(cid, 0);
        send_error(client_fd, E_BUSY, "System busy");
        return ;
    }
    
    memset(state, 0, sizeof(download_state_t));
    strncpy(state->cid, cid, 127);
    state->cid[127] = '\0';
    state->client_fd = client_fd;
    state->manifest = manifest_info;
    state->total_chunks = manifest_info->chunk_count;
    state->tasks_remaining = state->total_chunks;
    
    
    // تخصیص آرایه‌ها
    state->chunk_data = calloc(state->total_chunks, sizeof(uint8_t*));
    state->chunk_sizes = calloc(state->total_chunks, sizeof(size_t));
    state->chunk_ready = calloc(state->total_chunks, sizeof(int));
    state->chunk_paths = calloc(state->total_chunks, sizeof(char*));
    state->chunk_order = malloc(state->total_chunks * sizeof(int));
    
    if (!state->chunk_data || !state->chunk_sizes || !state->chunk_ready ||
        !state->chunk_paths || !state->chunk_order) {
        free_manifest_info(manifest_info);
        free(state->chunk_data);
        free(state->chunk_sizes);
        free(state->chunk_ready);
        free(state->chunk_paths);
        free(state->chunk_order);
        free(state);
        manifest_unlock(cid, 0);
        send_error(client_fd, E_BUSY, "System busy");
        return ;
    }
    
    // پر کردن آرایه مسیرها و اندیس‌ها
    for (int i = 0; i < state->total_chunks; i++) {
        state->chunk_paths[i] = strdup(manifest_info->chunks[i].file_path);
        state->chunk_order[i] = manifest_info->chunks[i].index;
        //printf("bef1");
        char hex[2*HASH_LENGTH+1];
        hash_to_hex(manifest_info->chunks[i].hash,hex);
        //printf("bef2");
        state->chunk_data[i]= read_block(hex, &manifest_info->chunks[i].size);
        state->chunk_ready[i] = 1;
        printf(state->chunk_data[i]);
        state->chunks_ready_count++;
        //state->chunk_sizes[i] = manifest_info->chunks[i].size;
    }
    
        // مقداردهی اولیه مکانیزم‌های همگام‌سازی
    pthread_mutex_init(&state->lock, NULL);
    pthread_cond_init(&state->chunk_ready_cond, NULL);
    pthread_cond_init(&state->chunk_sent_cond, NULL);
    pthread_cond_init(&state->all_tasks_done_cond, NULL);
    
    // AFTER ALL CHUNKS ARE LOADED, signal the condition variable
    pthread_mutex_lock(&state->lock);
    state->next_expected_index = 0;  // We're ready to send from index 0
    state->chunks_sent_count = 0;
    pthread_cond_signal(&state->chunk_ready_cond);  // WAKE UP THE WAITING THREAD!
    pthread_mutex_unlock(&state->lock);
    //state->chunks_ready_count = 0;
    //state->chunks_sent_count = 0;
    //state->next_expected_index = 0;
    
    // افزایش reference count برای تمام بلاک‌ها
    for (int i = 0; i < state->total_chunks; i++) {
        char hash_hex[65];
        hash_to_hex(state->manifest->chunks[i].hash, hash_hex);
        increment_ref_count(hash_hex);
    }
    
    // آزاد کردن قفل مانیفست
    manifest_unlock(cid, 0);
    
    printf("[ENGINE] Download started: %s, chunks: %d\n", cid, state->total_chunks);
    return ;//state;
}

// callback برای اتمام پردازش چانک دانلود
void download_chunk_callback(void *arg, int result) {
    int *chunk_index_ptr = (int*)arg;
    int chunk_index = *chunk_index_ptr;
    download_state_t *state = NULL;
    
    // یافتن state مربوطه (در این نسخه ساده، از آرگومان استفاده می‌کنیم)
    // در نسخه واقعی باید state را از جای دیگری بگیریم
    free(chunk_index_ptr);
}

// دانلود چانک‌ها با Thread Pool
int download_chunks(download_state_t *state) {
    if (!state) return -1;
    
    printf("[ENGINE] Starting parallel download of %d chunks\n", state->total_chunks);
    
    int tasks_submitted = 0;
    
    // تنظیم tasks_remaining
    pthread_mutex_lock(&state->lock);
    state->tasks_remaining = state->total_chunks;
    pthread_mutex_unlock(&state->lock);

    // برای هر چانک یک task ایجاد و به Thread Pool ارسال کن
    for (int i = 0; i < state->total_chunks; i++) {
        file_part_t *chunk = &state->manifest->chunks[i];
        
        // تبدیل hash به string hex
        char hash_hex[65];
        hash_to_hex(chunk->hash, hash_hex);
        
        // تعریف callback برای اطلاع اتمام
        void chunk_download_callback(void *arg, int result) {
            int *chunk_index_ptr = (int*)arg;
            int chunk_index = *chunk_index_ptr;
            
            pthread_mutex_lock(&state->lock);
            
            if (result == 0) {
                state->chunk_ready[chunk_index] = 1;
                state->chunks_ready_count++;
                
                // اگر این چانک، چانک بعدی مورد انتظار است، سیگنال بده
                if (chunk_index == state->next_expected_index) {
                    pthread_cond_signal(&state->chunk_ready_cond);
                }
            } else {
                state->chunk_ready[chunk_index] = -1;
                printf("[ERROR] Failed to process chunk %d\n", chunk_index);
            }

            // کاهش tasks_remaining
            state->tasks_remaining--;
            if (state->tasks_remaining == 0) {
                pthread_cond_signal(&state->all_tasks_done_cond);
            }

            pthread_mutex_unlock(&state->lock);
            free(chunk_index_ptr);
        }
        
        // ذخیره اندیس برای callback
        int *chunk_index_ptr = malloc(sizeof(int));
         if (!chunk_index_ptr) {
            printf("[ERROR] Failed to allocate memory for chunk index\n");
            continue;
        }
        *chunk_index_ptr = i;
        
        // ساخت مسیر فایل
        char block_path[512];
        snprintf(block_path, sizeof(block_path), 
                "blocks/%.2s/%.2s/%s",
                hash_hex, hash_hex + 2, hash_hex);
        

        // ایجاد task برای Thread Pool
        task_t *task = create_download_chunk_task(
            state, i, hash_hex, block_path ,
            chunk->size, chunk_download_callback, chunk_index_ptr
        );
        
        if (!task) {
            free(chunk_index_ptr);
            continue;
        }
        
        // ارسال task به Thread Pool
        if (thread_pool_submit_task(&g_thread_pool, task) == 0) {
            tasks_submitted++;
        } 
        else {
            printf("[ERROR] Failed to submit task for chunk %d\n", i);
            free(chunk_index_ptr);
            // free_task(task); // تابع free_task را صدا بزن
            if (task->type == TASK_TYPE_DOWNLOAD_CHUNK) {
                // هیچ داده‌ای برای آزادسازی وجود ندارد
            }
            free(task);
        }
    }
    
    printf("[ENGINE] %d/%d chunk tasks submitted\n", 
           tasks_submitted, state->total_chunks);
    
    return tasks_submitted;
}
/*
// ارسال چانک‌ها به ترتیب
void send_chunks_in_order(download_state_t *state) {
    int next_to_send = 0;
    
    while (next_to_send < state->total_chunks) {
        pthread_mutex_lock(&state->lock);
        
        // منتظر بمان تا چانک بعدی آماده شود
        while (state->chunk_ready[next_to_send] == 0) {
            pthread_cond_wait(&state->chunk_ready_cond, &state->lock);
        }
        
        // اگر چانک خطا داشته باشد
        if (state->chunk_ready[next_to_send] == -1) {
            pthread_mutex_unlock(&state->lock);
            printf("[ERROR] Chunk %d failed, skipping\n", next_to_send);
            next_to_send++;
            continue;
        }
        
        // چانک آماده است - ارسال کن
        uint32_t index = htonl(next_to_send);
        uint32_t size = htonl(state->chunk_sizes[next_to_send]);
        
        // ساخت هدر
        uint8_t header[8];
        memcpy(header, &index, 4);
        memcpy(header + 4, &size, 4);
        
        
        // ارسال فریم با OP_DOWNLOAD_CHUNK
        uint8_t op = OP_DOWNLOAD_CHUNK;
        if (send_frame(state->client_fd, op, 
                      state->chunk_data[next_to_send],
                      state->chunk_sizes[next_to_send]) < 0) {
            pthread_mutex_unlock(&state->lock);
            printf("[ERROR] Failed to send chunk %d\n", next_to_send);
            break;
        }

        printf("[ENGINE] Sent chunk %d (%zu bytes)\n", 
               next_to_send, state->chunk_sizes[next_to_send]);
        
        // آزادسازی حافظه چانک ارسال شده
        free(state->chunk_data[next_to_send]);
        state->chunk_data[next_to_send] = NULL;

        next_to_send++;
        state->next_expected_index = next_to_send;
        
        pthread_mutex_unlock(&state->lock);
    }
}
*/
// ارسال چانک‌ها به ترتیب
void send_chunks_in_order(download_state_t *state) {
    int next_to_send = 0;
    
    while (next_to_send < state->total_chunks) {
        pthread_mutex_lock(&state->lock);
        
        // منتظر بمان تا چانک بعدی آماده شود
        while (state->chunk_ready[next_to_send] == 0) {
            printf("[ERROR] Chunk %d not ready, waiting\n", next_to_send);
            pthread_cond_wait(&state->chunk_ready_cond, &state->lock);
        }
        
        // اگر چانک خطا داشته باشد
        if (state->chunk_ready[next_to_send] == -1) {
            pthread_mutex_unlock(&state->lock);
            printf("[ERROR] Chunk %d failed, skipping\n", next_to_send);
            next_to_send++;
            continue;
        }
        
        // چانک آماده است - ساخت payload
        uint32_t index = htonl(next_to_send);
        uint32_t size = htonl(state->chunk_sizes[next_to_send]);
        
        // ساخت payload: [index(4)][size(4)][data...]
        uint8_t *payload = malloc(8 + state->chunk_sizes[next_to_send]);
        if (!payload) {
            pthread_mutex_unlock(&state->lock);
            printf("[ERROR] Failed to allocate payload for chunk %d\n", next_to_send);  
            // Clean up the chunk data
            free(state->chunk_data[next_to_send]);
            state->chunk_data[next_to_send] = NULL;
            next_to_send++;
            continue;

            //break;
        }
        
        memcpy(payload, &index, 4);
        memcpy(payload + 4, &size, 4);
        memcpy(payload + 8, state->chunk_data[next_to_send], state->chunk_sizes[next_to_send]);
        
        // ارسال فریم
        if (send_frame(state->client_fd, OP_DOWNLOAD_CHUNK, payload, 
                      8 + state->chunk_sizes[next_to_send]) < 0) {
            free(payload);
            pthread_mutex_unlock(&state->lock);
            printf("[ERROR] Failed to send chunk %d\n", next_to_send);
            break;
        }
        
        free(payload);
        
        printf("[ENGINE] Sent chunk %d (%zu bytes)\n", 
               next_to_send, state->chunk_sizes[next_to_send]);
        
        // آزادسازی حافظه چانک ارسال شده
        free(state->chunk_data[next_to_send]);
        state->chunk_data[next_to_send] = NULL;
        
        next_to_send++;
        state->chunks_sent_count = next_to_send;
        state->next_expected_index = next_to_send;
        
        pthread_mutex_unlock(&state->lock);
    }
}
// پایان دانلود
void download_done(download_state_t *state) {
    if (!state) return;
    
    printf("[ENGINE] Finalizing download for CID: %s\n", state->cid);
    
    // ارسال پیام اتمام
    send_frame(state->client_fd, OP_DOWNLOAD_DONE, NULL, 0);
    
    // کاهش refcount بلاک‌ها
    for (int i = 0; i < state->total_chunks; i++) {
        char hash_hex[65];
        hash_to_hex(state->manifest->chunks[i].hash, hash_hex);
        decrement_ref_count(hash_hex);
    }
    
    // پاکسازی
    free_manifest_info(state->manifest);
    free(state->cid);
    
    if (state->chunk_data) {
        for (int i = 0; i < state->total_chunks; i++) {
            free(state->chunk_data[i]);
        }
        free(state->chunk_data);
    }
    
    free(state->chunk_sizes);
    free(state->chunk_ready);
    
    if (state->chunk_paths) {
        for (int i = 0; i < state->total_chunks; i++) {
            free(state->chunk_paths[i]);
        }
        free(state->chunk_paths);
    }
    
    free(state->chunk_order);
    
    pthread_mutex_destroy(&state->lock);
    pthread_cond_destroy(&state->chunk_ready_cond);
    pthread_cond_destroy(&state->chunk_sent_cond);
    
    free(state);
    
    printf("[ENGINE] Download completed\n");
}

// ==================== پردازش اتصال ====================

void handle_connection(int client_fd) {
    upload_state_t *upload_state = NULL;
    download_state_t *download_state = NULL;
    
    printf("[ENGINE] New connection from client\n");
    
    for (;;) {
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
        
        switch (opcode) {
            case OP_UPLOAD_START:
                if (upload_state) {
                    send_error(client_fd, E_PROTO, "Upload already in progress");
                } else {
                    upload_state = handle_upload_start(client_fd, payload, length);
                    if (!upload_state) {
                        // خطا قبلاً ارسال شده
                    }
                }
                break;
                
            case OP_UPLOAD_CHUNK:
                handle_upload_chunk(client_fd, payload, length, upload_state);
                break;
                
            case OP_UPLOAD_FINISH:
                handle_upload_finish(client_fd, upload_state);
                upload_state = NULL;
                // بعد از اتمام آپلود، اتصال را نگه می‌داریم برای عملیات بعدی
                break;
            case OP_DOWNLOAD_START: {
                if (payload == NULL || length == 0) {
                    send_error(client_fd, E_PROTO, "No CID provided");
                    break;
                }
                
                char *cid = malloc(length + 1);
                memcpy(cid, payload, length);
                cid[length] = '\0';
                
                
                
                // Start the download process
                download_state = malloc(sizeof(download_state_t));
                memset(download_state, 0, sizeof(download_state_t));
                strncpy(download_state->cid, cid, 127);
                download_state->client_fd = client_fd;
                
                // Get manifest and start download
                download_start(client_fd, cid, download_state);
                
                if (/*download_start(client_fd, cid, state) == 0 &&*/ download_state->manifest) {
                    // Start parallel download
                    printf("real start");
                    int tasks_submitted = download_chunks(download_state);
                    
                    if (tasks_submitted > 0) {
                        // Send chunks in order
                        send_chunks_in_order(download_state);
                        
                        
                    } else {
                        send_error(client_fd, E_BUSY, "Failed to start download");
                    }
                    
                    // Clean up
                    download_done(download_state);
                } else {
                    // Error already sent by download_start
                    printf("NOOT");
                    free(download_state);
                }
                
                free(cid);
                break;
            } 
            /*case OP_DOWNLOAD_START: {
                if (payload == NULL || length == 0) {
                    send_error(client_fd, E_PROTO, "No CID provided");
                    break;
                }
                
                char *cid = malloc(length + 1);
                memcpy(cid, payload, length);
                cid[length] = '\0';
                
                download_start(client_fd, cid, download_state);
                free(cid);
                
                
                break;
            }
            case OP_DOWNLOAD_CHUNK :{
                if (download_state) {
                    printf("in it");
                    // شروع دانلود موازی
                    download_chunks(download_state);
                    
                    // ارسال چانک‌ها به ترتیب
                    send_chunks_in_order(download_state);
                    
                }
                break;

            }
            case OP_DOWNLOAD_DONE :
            {
                if (download_state) {
                    printf("in it")
                    // شروع دانلود موازی
                    download_chunks(download_state);
                    
                    // ارسال چانک‌ها به ترتیب
                    send_chunks_in_order(download_state);
                    
                    // پایان دانلود
                    download_done(download_state);
                    download_state = NULL;
                    
                    // بعد از اتمام دانلود، اتصال را نگه می‌داریم
                }
                break;
            }*/
                
            default:
                printf("[ENGINE] Unknown opcode: 0x%02x\n", opcode);
                send_error(client_fd, E_PROTO, "Invalid opcode");
                break;
        }
        
        if (payload) free(payload);
    }
    
    // پاکسازی در صورت خروج غیرعادی
    if (upload_state) {
        // پاکسازی upload_state
        free(upload_state->chunks);
        pthread_mutex_destroy(&upload_state->lock);
        pthread_cond_destroy(&upload_state->all_chunks_done);
        free(upload_state);
    }
    
    close(client_fd);
    printf("[ENGINE] Connection closed\n");
}

// تابع free_task
void free_task(task_t *task) {
    if (!task) return;
    
    switch (task->type) {
        case TASK_TYPE_UPLOAD_CHUNK:
            free(task->data.upload.data);
            break;
        case TASK_TYPE_WRITE_FILE:
            free(task->data.write_file.data);
            free(task->data.write_file.file_path);
            break;
        case TASK_TYPE_DELETE_FILE:
            free(task->data.delete_file.file_path);
            break;
        default:
            break;
    }
    free(task);
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
    if (thread_pool_init(&g_thread_pool, MAX_THREADS, MAX_TASKS) < 0) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        return 1;
    }
    
    // راه‌اندازی سیستم قفل‌ها
    if (manifest_lock_init() < 0) {
        fprintf(stderr, "Failed to initialize manifest locks\n");
        thread_pool_shutdown(&g_thread_pool);
        return 1;
    }
    //hash
    // ایجاد سوکت
    int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        thread_pool_shutdown(&g_thread_pool);
        manifest_lock_destroy();
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
        thread_pool_shutdown(&g_thread_pool);
        manifest_lock_destroy();
        return 1;
    }
    
    if (listen(server_fd, 64) < 0) {
        perror("listen");
        close(server_fd);
        unlink(g_sock_path);
        thread_pool_shutdown(&g_thread_pool);
        manifest_lock_destroy();
        return 1;
    }
    
    printf("[ENGINE] Server started on %s\n", g_sock_path);
    printf("[ENGINE] Thread pool: %d threads\n", MAX_THREADS);
    printf("[ENGINE] Chunk size: %d KB\n", CHUNK_SIZE / 1024);
    printf("[ENGINE] Hash algorithm: %s\n", HASH_ALGO);
    
    // حلقه اصلی پذیرش اتصال
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
    thread_pool_shutdown(&g_thread_pool);
    manifest_lock_destroy();
    
    // پاکسازی جدول‌ها
    for (int i = 0; i < manifest_table_size; i++) {
        free_manifest_info(manifest_table[i]->manifest_info);
        pthread_mutex_destroy(&manifest_table[i]->lock);
        free(manifest_table[i]);
    }
    free(manifest_table);
    
    for (int i = 0; i < chunk_table_size; i++) {
        free(chunk_table[i]->file_path);
        pthread_mutex_destroy(&chunk_table[i]->lock);
        free(chunk_table[i]);
    }
    free(chunk_table);
    
    printf("[ENGINE] Server shutdown complete\n");
    return 0;
}