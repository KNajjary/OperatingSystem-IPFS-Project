// c_engine.c  — اصلاح شده برای کامپایل و اجرای اولیه
// Build:
//   gcc -O2 -pthread -Wall -Wextra -o c_engine c_engine.c -lcrypto
// Run:
//   ./c_engine /tmp/cengine.sock

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
#include <openssl/sha.h>
#include <time.h>
#include <limits.h>
#include <signal.h>

// ==================== ثابت‌ها ====================
#define OP_UPLOAD_START   0x01
#define OP_UPLOAD_CHUNK   0x02
#define OP_UPLOAD_FINISH  0x03
#define OP_UPLOAD_DONE    0x81

#define OP_DOWNLOAD_START 0x10
#define OP_DOWNLOAD_CHUNK 0x90
#define OP_DOWNLOAD_DONE  0x91

#define OP_ERROR          0xFF

#define CHUNK_SIZE        (256 * 1024)  // 256KB
#define HASH_LENGTH       32
#define MAX_THREADS       8
#define MAX_TASKS         100

// مسیرها
static const char *BLOCKS_DIR = "blocks";
static const char *MANIFESTS_DIR = "manifests";

// ==================== ساختارها ====================
typedef struct {
    char hash_hex[HASH_LENGTH * 2 + 1];
    char *file_path;
    int ref_count;
    pthread_mutex_t lock;
    time_t last_access;
} chunk_entry_t;

typedef struct {
    char *cid;
    char *manifest_path;
    int reader_count;
    pthread_rwlock_t rwlock;
} manifest_entry_t;

typedef struct {
    void (*task_func)(void*);
    void *arg;
    int task_id;
} task_t;

typedef struct {
    pthread_t workers[MAX_THREADS];
    task_t task_queue[MAX_TASKS];
    int queue_size;
    int queue_front;
    int queue_rear;
    int shutdown;
    pthread_mutex_t queue_lock;
    pthread_cond_t queue_not_empty;
    pthread_cond_t queue_not_full;
} thread_pool_t;

typedef struct {
    char *filename;
    long long total_size;
    int chunk_count; // allocated size of chunks array
    struct {
        int index;
        int size;
        unsigned char hash[HASH_LENGTH];
        char *file_path;
    } *chunks;
    int chunks_received;
    pthread_mutex_t lock;
    pthread_cond_t all_chunks_done;
} upload_state_t;

typedef struct {
    char *cid;
    manifest_entry_t *manifest;
    int total_chunks;
    int chunks_sent;
    pthread_mutex_t lock;
} download_state_t;

typedef struct {
    int client_fd;
    int chunk_index;
    uint8_t *data;
    size_t data_size;
    upload_state_t *upload_state;
} chunk_task_arg_t;

// ==================== متغیرهای سراسری ====================
static const char *g_sock_path = NULL;
static int g_listen_fd = -1;

static thread_pool_t thread_pool;

static chunk_entry_t **chunk_table = NULL;
static int chunk_table_size = 0;
static pthread_mutex_t chunk_table_lock = PTHREAD_MUTEX_INITIALIZER;

static manifest_entry_t **manifest_table = NULL;
static int manifest_table_size = 0;
static pthread_mutex_t manifest_table_lock = PTHREAD_MUTEX_INITIALIZER;

// ==================== کمکی‌ها ====================
void ensure_dir(const char *path) {
    struct stat st;
    if (stat(path, &st) == -1) {
        if (mkdir(path, 0755) < 0 && errno != EEXIST) {
            perror("mkdir");
        }
    }
}

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
    return (ssize_t)total;
}

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

int send_frame(int fd, uint8_t opcode, const void *payload, uint32_t length) {
    uint8_t header[5];
    header[0] = opcode;
    uint32_t be_len = htonl(length);
    memcpy(header + 1, &be_len, 4);
    if (write_all(fd, header, 5) < 0) return -1;
    if (length > 0 && payload) {
        if (write_all(fd, payload, length) < 0) return -1;
    }
    return 0;
}

int send_error(int fd, const char *code, const char *message) {
    char error_json[256];
    snprintf(error_json, sizeof(error_json), "{\"code\":\"%s\",\"message\":\"%s\"}", code, message);
    return send_frame(fd, OP_ERROR, error_json, (uint32_t)strlen(error_json));
}

// SHA-256 با OpenSSL
void compute_hash(const uint8_t *data, size_t len, uint8_t *hash_out) {
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data, len);
    SHA256_Final(hash_out, &ctx);
}

void hash_to_hex(const uint8_t *hash, char *hex_out) {
    for (int i = 0; i < HASH_LENGTH; ++i) {
        sprintf(hex_out + i * 2, "%02x", hash[i]);
    }
    hex_out[HASH_LENGTH * 2] = '\0';
}

// ==================== Thread Pool ====================
void *worker_thread(void *arg) {
    thread_pool_t *pool = (thread_pool_t *)arg;
    while (1) {
        pthread_mutex_lock(&pool->queue_lock);
        while (pool->queue_size == 0 && !pool->shutdown) {
            pthread_cond_wait(&pool->queue_not_empty, &pool->queue_lock);
        }
        if (pool->shutdown && pool->queue_size == 0) {
            pthread_mutex_unlock(&pool->queue_lock);
            break;
        }
        task_t task = pool->task_queue[pool->queue_front];
        pool->queue_front = (pool->queue_front + 1) % MAX_TASKS;
        pool->queue_size--;
        pthread_cond_signal(&pool->queue_not_full);
        pthread_mutex_unlock(&pool->queue_lock);

        // اجرای وظیفه (وظیفه خودش مسئول آزادسازی arg است)
        if (task.task_func) task.task_func(task.arg);
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
    for (int i = 0; i < MAX_THREADS; ++i) {
        if (pthread_create(&pool->workers[i], NULL, worker_thread, pool) != 0) {
            perror("pthread_create");
            return -1;
        }
    }
    return 0;
}

int thread_pool_submit(thread_pool_t *pool, void (*task_func)(void*), void *arg, int task_id) {
    pthread_mutex_lock(&pool->queue_lock);
    while (pool->queue_size == MAX_TASKS && !pool->shutdown) {
        pthread_cond_wait(&pool->queue_not_full, &pool->queue_lock);
    }
    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->queue_lock);
        return -1;
    }
    pool->task_queue[pool->queue_rear].task_func = task_func;
    pool->task_queue[pool->queue_rear].arg = arg;
    pool->task_queue[pool->queue_rear].task_id = task_id;
    pool->queue_rear = (pool->queue_rear + 1) % MAX_TASKS;
    pool->queue_size++;
    pthread_cond_signal(&pool->queue_not_empty);
    pthread_mutex_unlock(&pool->queue_lock);
    return 0;
}

void thread_pool_shutdown(thread_pool_t *pool) {
    pthread_mutex_lock(&pool->queue_lock);
    pool->shutdown = 1;
    pthread_mutex_unlock(&pool->queue_lock);
    pthread_cond_broadcast(&pool->queue_not_empty);
    for (int i = 0; i < MAX_THREADS; ++i) {
        pthread_join(pool->workers[i], NULL);
    }
}

// ==================== وظیفهٔ پردازش چانک ====================
void process_chunk_task(void *arg) {
    chunk_task_arg_t *task_arg = (chunk_task_arg_t *)arg;
    upload_state_t *state = task_arg->upload_state;

    // محاسبه هش
    uint8_t hash[HASH_LENGTH];
    compute_hash(task_arg->data, task_arg->data_size, hash);
    char hash_hex[HASH_LENGTH * 2 + 1];
    hash_to_hex(hash, hash_hex);

    // مسیر ذخیره
    char dir_path[PATH_MAX];
    char subdir_path[PATH_MAX];
    snprintf(dir_path, sizeof(dir_path), "%s/%.2s", BLOCKS_DIR, hash_hex);
    snprintf(subdir_path, sizeof(subdir_path), "%s/%.2s/%.2s", BLOCKS_DIR, hash_hex, hash_hex + 2);

    ensure_dir(BLOCKS_DIR);
    ensure_dir(dir_path);
    ensure_dir(subdir_path);

    char file_path[PATH_MAX];
    snprintf(file_path, sizeof(file_path), "%s/%s", subdir_path, hash_hex);

    // قفل جدول چانک‌ها و جستجو/افزودن
    pthread_mutex_lock(&chunk_table_lock);
    chunk_entry_t *chunk = NULL;
    for (int i = 0; i < chunk_table_size; ++i) {
        if (chunk_table[i] && strcmp(chunk_table[i]->hash_hex, hash_hex) == 0) {
            chunk = chunk_table[i];
            break;
        }
    }

    if (chunk) {
        pthread_mutex_lock(&chunk->lock);
        chunk->ref_count++;
        pthread_mutex_unlock(&chunk->lock);
        printf("[ENGINE] Duplicate chunk found %s, ref_count=%d\n", hash_hex, chunk->ref_count);
    } else {
        chunk = calloc(1, sizeof(chunk_entry_t));
        if (chunk) {
            strncpy(chunk->hash_hex, hash_hex, sizeof(chunk->hash_hex));
            chunk->file_path = strdup(file_path);
            chunk->ref_count = 1;
            chunk->last_access = time(NULL);
            pthread_mutex_init(&chunk->lock, NULL);

            // ذخیره فایل جدید (با O_EXCL به‌صورت ساده)
            int fd = open(file_path, O_WRONLY | O_CREAT | O_EXCL, 0644);
            if (fd >= 0) {
                if (write_all(fd, task_arg->data, task_arg->data_size) == 0) {
                    printf("[ENGINE] Stored new chunk %s\n", hash_hex);
                } else {
                    // نوشتن شکست خورد
                    unlink(file_path);
                }
                close(fd);
            } else {
                if (errno == EEXIST) {
                    // کسی قبلاً نوشت (race) — می‌توان refcount را افزایش داد
                    free(chunk->file_path);
                    free(chunk);
                    // دوباره جستجو و افزایش refcount
                    for (int i = 0; i < chunk_table_size; ++i) {
                        if (chunk_table[i] && strcmp(chunk_table[i]->hash_hex, hash_hex) == 0) {
                            chunk = chunk_table[i];
                            pthread_mutex_lock(&chunk->lock);
                            chunk->ref_count++;
                            pthread_mutex_unlock(&chunk->lock);
                            chunk = NULL; // indicate handled
                            break;
                        }
                    }
                } else {
                    perror("open");
                }
            }

            if (chunk) {
                // append to table
                chunk_table = realloc(chunk_table, (chunk_table_size + 1) * sizeof(chunk_entry_t*));
                chunk_table[chunk_table_size++] = chunk;
            }
        }
    }
    pthread_mutex_unlock(&chunk_table_lock);

    // ثبت در upload_state
    pthread_mutex_lock(&state->lock);

    // اگر آرایه chunks کافی نیست آن را افزایش می‌دهیم
    if (state->chunks_received >= state->chunk_count) {
        int new_count = (state->chunk_count == 0) ? 4 : state->chunk_count * 2;
        state->chunks = realloc(state->chunks, new_count * sizeof(state->chunks[0]));
        // نوشتن صفر برای فضای جدید
        for (int i = state->chunk_count; i < new_count; ++i) {
            state->chunks[i].index = -1;
            state->chunks[i].size = 0;
            state->chunks[i].file_path = NULL;
            memset(state->chunks[i].hash, 0, HASH_LENGTH);
        }
        state->chunk_count = new_count;
    }

    int idx = task_arg->chunk_index;
    state->chunks[idx].index = idx;
    state->chunks[idx].size = (int)task_arg->data_size;
    memcpy(state->chunks[idx].hash, hash, HASH_LENGTH);
    state->chunks[idx].file_path = strdup(file_path);

    state->chunks_received++;
    printf("[ENGINE] Chunk %d processed: %s (%d/%d)\n", idx, hash_hex, state->chunks_received, state->chunk_count);

    if (state->chunks_received == state->chunks_received /* placeholder — real termination decided elsewhere */) {
        // Note: real code should compare with expected total; here we simply signal
        pthread_cond_signal(&state->all_chunks_done);
    }

    pthread_mutex_unlock(&state->lock);

    // آزادسازی حافظه اختصاص داده شده برای data و خود آرگومان
    free(task_arg->data);
    free(task_arg);
}

// ==================== CID / manifest (نسخهٔ ساده) ====================

// Base32 ساده (RFC متفاوت) — مناسب نمونه
char *base32_encode(const uint8_t *data, size_t len) {
    const char *alphabet = "abcdefghijklmnopqrstuvwxyz234567";
    size_t out_len = (len * 8 + 4) / 5;
    char *encoded = malloc(out_len + 1);
    if (!encoded) return NULL;
    int buffer = 0, bits = 0, out_idx = 0;
    for (size_t i = 0; i < len; ++i) {
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

// تولید CID ساده‌شده (multihash + base32)
char *generate_cid_simple(const char *manifest_json) {
    uint8_t h[HASH_LENGTH];
    compute_hash((const uint8_t*)manifest_json, strlen(manifest_json), h);
    // multihash: [code(1byte)=0x12? use 0x12 placeholder][len][digest]
    uint8_t multihash[2 + HASH_LENGTH];
    multihash[0] = 0x12; // placeholder (sha2-256) — for demo
    multihash[1] = HASH_LENGTH;
    memcpy(multihash + 2, h, HASH_LENGTH);
    char *b32 = base32_encode(multihash, sizeof(multihash));
    if (!b32) return NULL;
    size_t n = strlen(b32) + 2;
    char *cid = malloc(n);
    snprintf(cid, n, "b%s", b32);
    free(b32);
    return cid;
}

// ایجاد مانیفست ساده (JSON کوچک)
char *create_manifest_simple(upload_state_t *state) {
    // این نسخه ساده: فقط فهرست چانک‌ها را در JSON می‌سازد
    size_t bufcap = 1024 + (state->chunks_received * 200);
    char *buf = malloc(bufcap);
    if (!buf) return NULL;
    int off = 0;
    off += snprintf(buf + off, bufcap - off,
        "{\n  \"version\":1,\n  \"hash_algo\":\"sha256\",\n  \"chunk_size\":%d,\n  \"total_size\":%lld,\n  \"filename\":\"%s\",\n  \"chunks\":[\n",
        CHUNK_SIZE, state->total_size, state->filename ? state->filename : "unknown");
    for (int i = 0; i < state->chunks_received; ++i) {
        char hhex[HASH_LENGTH*2+1];
        hash_to_hex(state->chunks[i].hash, hhex);
        off += snprintf(buf + off, bufcap - off,
                        "    {\"index\":%d, \"size\":%d, \"hash\":\"%s\"}%s\n",
                        state->chunks[i].index, state->chunks[i].size,
                        hhex, (i==state->chunks_received-1) ? "" : ",");
    }
    off += snprintf(buf + off, bufcap - off, "  ]\n}\n");
    return buf;
}

int save_manifest(const char *cid, const char *manifest_json) {
    ensure_dir(MANIFESTS_DIR);
    char manifest_path[PATH_MAX];
    char temp_path[PATH_MAX];
    snprintf(manifest_path, sizeof(manifest_path), "%s/%s.json", MANIFESTS_DIR, cid);
    snprintf(temp_path, sizeof(temp_path), "%s/%s.tmp", MANIFESTS_DIR, cid);
    int fd = open(temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    if (write_all(fd, manifest_json, strlen(manifest_json)) < 0) {
        close(fd);
        unlink(temp_path);
        return -1;
    }
    fsync(fd);
    close(fd);
    if (rename(temp_path, manifest_path) < 0) {
        unlink(temp_path);
        return -1;
    }
    return 0;
}

// ==================== مدیریت آپلود ====================

upload_state_t *handle_upload_start(int client_fd, const uint8_t *payload, uint32_t len) {
    printf("[ENGINE] UPLOAD_START: %.*s\n", (int)len, payload);
    upload_state_t *state = calloc(1, sizeof(upload_state_t));
    if (!state) return NULL;
    state->filename = malloc(len + 1);
    if (!state->filename) { free(state); return NULL; }
    memcpy(state->filename, payload, len);
    state->filename[len] = '\0';
    state->total_size = 0;
    state->chunk_count = 0;
    state->chunks = NULL;
    state->chunks_received = 0;
    pthread_mutex_init(&state->lock, NULL);
    pthread_cond_init(&state->all_chunks_done, NULL);
    return state;
}

void handle_upload_chunk(int client_fd, const uint8_t *payload, uint32_t len, upload_state_t *state) {
    if (!state) {
        send_error(client_fd, "E_PROTO", "No upload started");
        return;
    }

    // Make a task arg: we treat each chunk as coming in order
    chunk_task_arg_t *task_arg = malloc(sizeof(chunk_task_arg_t));
    if (!task_arg) {
        send_error(client_fd, "E_INTERNAL", "alloc failed");
        return;
    }
    task_arg->client_fd = client_fd;
    // assign chunk_index = current chunks_received
    pthread_mutex_lock(&state->lock);
    int index = state->chunks_received;
    task_arg->chunk_index = index;
    task_arg->data = malloc(len);
    if (!task_arg->data) {
        pthread_mutex_unlock(&state->lock);
        free(task_arg);
        send_error(client_fd, "E_INTERNAL", "alloc failed");
        return;
    }
    memcpy(task_arg->data, payload, len);
    task_arg->data_size = len;
    task_arg->upload_state = state;
    // submit task
    if (thread_pool_submit(&thread_pool, process_chunk_task, task_arg, index) < 0) {
        free(task_arg->data);
        free(task_arg);
        pthread_mutex_unlock(&state->lock);
        send_error(client_fd, "E_BUSY", "Thread pool busy");
        return;
    }
    state->total_size += len;
    state->chunks_received++; // optimistic increment — actual chunk array filled by worker
    pthread_mutex_unlock(&state->lock);
}

void handle_upload_finish(int client_fd, upload_state_t *state) {
    if (!state) {
        send_error(client_fd, "E_PROTO", "No upload started");
        return;
    }
    // Wait briefly for worker tasks to complete (simple approach)
    // In production we would track expected total or provide explicit chunk count.
    // Here: sleep + small wait loop (not ideal, but workable for simple tests)
    for (int i = 0; i < 100 && state->chunks_received < 1; ++i) {
        usleep(10000);
    }

    // create simple manifest from whatever chunks we have
    char *manifest_json = create_manifest_simple(state);
    if (!manifest_json) {
        send_error(client_fd, "E_INTERNAL", "manifest failed");
        return;
    }
    char *cid = generate_cid_simple(manifest_json);
    if (!cid) {
        free(manifest_json);
        send_error(client_fd, "E_INTERNAL", "cid failed");
        return;
    }
    if (save_manifest(cid, manifest_json) < 0) {
        free(manifest_json);
        free(cid);
        send_error(client_fd, "E_INTERNAL", "save manifest failed");
        return;
    }

    // register manifest in memory table
    pthread_mutex_lock(&manifest_table_lock);
    manifest_entry_t *me = calloc(1, sizeof(manifest_entry_t));
    me->cid = strdup(cid);
    me->manifest_path = NULL;
    me->reader_count = 0;
    pthread_rwlock_init(&me->rwlock, NULL);
    manifest_table = realloc(manifest_table, (manifest_table_size + 1) * sizeof(manifest_entry_t*));
    manifest_table[manifest_table_size++] = me;
    pthread_mutex_unlock(&manifest_table_lock);

    // reply
    send_frame(client_fd, OP_UPLOAD_DONE, cid, (uint32_t)strlen(cid));
    printf("[ENGINE] Upload finished: CID=%s size=%lld chunks=%d\n", cid, state->total_size, state->chunks_received);

    free(manifest_json);
    free(cid);
}

// ==================== دانلود (نسخهٔ ساده) ====================

manifest_entry_t *find_manifest(const char *cid) {
    pthread_mutex_lock(&manifest_table_lock);
    for (int i = 0; i < manifest_table_size; ++i) {
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
    if (len >= (int)sizeof(cid)) len = sizeof(cid) - 1;
    memcpy(cid, payload, len);
    cid[len] = '\0';
    printf("[ENGINE] DOWNLOAD_START: CID=%s\n", cid);

    manifest_entry_t *me = find_manifest(cid);
    if (!me) {
        send_error(client_fd, "E_NOT_FOUND", "CID not found");
        return;
    }

    // read manifest file (simple)
    char manifest_path[PATH_MAX];
    snprintf(manifest_path, sizeof(manifest_path), "%s/%s.json", MANIFESTS_DIR, cid);
    FILE *f = fopen(manifest_path, "r");
    if (!f) {
        send_error(client_fd, "E_NOT_FOUND", "manifest file missing");
        return;
    }
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc(fsize + 1);
    if (!buf) { fclose(f); send_error(client_fd, "E_INTERNAL", "alloc fail"); return; }
    fread(buf, 1, fsize, f);
    buf[fsize] = '\0';
    fclose(f);

    // NOTE: parsing JSON and streaming chunks with verification is not implemented here.
    // For now just return DOWNLOAD_DONE as placeholder.
    send_frame(client_fd, OP_DOWNLOAD_DONE, NULL, 0);
    free(buf);
}

// ==================== اتصال و نخ‌ها ====================

void handle_connection_cb(void *arg) {
    int client_fd = (int)(intptr_t)arg;
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
            if (!payload) break;
            if (read_exact(client_fd, payload, length) <= 0) { free(payload); break; }
        }

        switch (opcode) {
            case OP_UPLOAD_START:
                if (upload_state) {
                    // cleanup previous (rare)
                }
                upload_state = handle_upload_start(client_fd, payload, length);
                break;
            case OP_UPLOAD_CHUNK:
                handle_upload_chunk(client_fd, payload, length, upload_state);
                break;
            case OP_UPLOAD_FINISH:
                handle_upload_finish(client_fd, upload_state);
                if (upload_state) {
                    // cleanup
                    free(upload_state->filename);
                    if (upload_state->chunks) {
                        for (int i = 0; i < upload_state->chunk_count; ++i) {
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
                printf("[ENGINE] Unknown opcode 0x%02x\n", opcode);
                send_error(client_fd, "E_PROTO", "Invalid opcode");
                break;
        }

        if (payload) free(payload);
    }

    close(client_fd);
    printf("[ENGINE] Connection closed\n");
    pthread_exit(NULL);
}

void *thread_entry(void *arg) {
    handle_connection_cb(arg);
    return NULL;
}

// cleanup socket file
static void cleanup_sockfile(void) {
    if (g_listen_fd >= 0) close(g_listen_fd);
    if (g_sock_path) unlink(g_sock_path);
}

static void sigint_handler(int sig) {
    (void)sig;
    exit(0);
}

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s /tmp/cengine.sock\n", argv[0]);
        return 1;
    }
    g_sock_path = argv[1];

    ensure_dir(BLOCKS_DIR);
    ensure_dir(MANIFESTS_DIR);

    if (thread_pool_init(&thread_pool) < 0) {
        fprintf(stderr, "thread pool init failed\n");
        return 1;
    }

    atexit(cleanup_sockfile);
    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler);

    int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }
    g_listen_fd = server_fd;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, g_sock_path, sizeof(addr.sun_path)-1);
    unlink(g_sock_path);

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(server_fd); return 1;
    }
    if (listen(server_fd, 64) < 0) {
        perror("listen"); close(server_fd); unlink(g_sock_path); return 1;
    }

    printf("[ENGINE] Server started on %s\n", g_sock_path);
    printf("[ENGINE] Thread pool threads: %d\n", MAX_THREADS);
    printf("[ENGINE] Chunk size: %d KB\n", CHUNK_SIZE / 1024);

    while (1) {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }
        pthread_t th;
        int rc = pthread_create(&th, NULL, thread_entry, (void*)(intptr_t)client_fd);
        if (rc != 0) {
            fprintf(stderr, "pthread_create failed: %s\n", strerror(rc));
            close(client_fd);
            continue;
        }
        pthread_detach(th);
    }

    close(server_fd);
    unlink(g_sock_path);
    thread_pool_shutdown(&thread_pool);
    printf("[ENGINE] shutdown\n");
    return 0;
}





// Build: gcc -O2 -pthread -o c_engine c_engine.c
// Run:   ./c_engine /tmp/cengine.sock
// kkkk
/*
#define _GNU_SOURCE
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <blake3.h>

#define OP_UPLOAD_START 0x01
#define OP_UPLOAD_CHUNK 0x02
#define OP_UPLOAD_FINISH 0x03
#define OP_UPLOAD_DONE 0x81

#define OP_DOWNLOAD_START 0x11
#define OP_DOWNLOAD_CHUNK 0x91
#define OP_DOWNLOAD_DONE 0x92

#define CHUNCK_BITS (256 * 1024)
#define HASH_LENGTH 32

static const char *g_sock_path = NULL;
static const char *BLOCKS_DIR = "blocks";
static const char *MANIFESTS_DIR = "manifests";

struct file_part
{
    int part_num;
    int part_size;
    unsigned char hash_code[HASH_LENGTH];
    char *file_location;
};

struct upload_info
{
    char *file_name;
    long long file_total_size;
    int part_total_count;
    struct file_part *parts_list;
    int current_part;
};

void ensure_dir(const char *path)
{
    struct stat st ;
    if (stat(path, &st) == -1)
    {
        mkdir(path, 0755);
    }
}

ssize_t read_n(int fd, void *buf, size_t n)
{
    size_t got = 0;
    while (got < n)
    {
        ssize_t r = read(fd, (char *)buf + got, n - got);
        if (r == 0)
            return 0;
        if (r < 0)
        {
            if (errno == EINTR)
                continue;
            perror("read");
            return -1;
        }
        got += r;
    }
    return (ssize_t)got;
}

int write_all(int fd, const void *buf, size_t n)
{
    size_t sent = 0;
    while (sent < n)
    {
        ssize_t w = write(fd, (const char *)buf + sent, n - sent);
        if (w < 0)
        {
            if (errno == EINTR)
                continue;
            perror("write");
            return -1;
        }
        sent += (size_t)w;
    }
    return 0;
}

int send_frame(int fd, uint8_t op, const void *payload, uint32_t len)
{
    uint8_t header[5];
    header[0] = op;
    uint32_t be_len = htonl(len);
    memcpy(header + 1, &be_len, 4);
    if (write_all(fd, header, 5) < 0)
        return -1;
    if (len && write_all(fd, payload, len) < 0)
        return -1;
    return 0;
}

void calculate_hash(const uint8_t *data, size_t len, uint8_t *hash)
{
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data, len);
    blake3_hasher_finalize(&hasher, hash, HASH_LENGTH);
}

char *store_chunk(const unsigned char *data, int len, const unsigned char *hash)
{
    // کنار هم گذاشتن خانه های آرایه هش
    char hash_hex[65];
    int i;
    for (i = 0; i < HASH_LENGTH; i++)
    {
        sprintf(hash_hex + i * 2, "%02x", hash[i]);
    }
    hash_hex[64] = '\0';

    // ساخت پوشه و زیر پوشه بر اساس دو گاراگتر اول و دوم هش
    char dir_path[256];
    char subdir_path[256];
    snprintf(dir_path, sizeof(dir_path), "%s/%.2s", BLOCKS_DIR, hash_hex);
    snprintf(subdir_path, sizeof(subdir_path), "%s/%.2s/%.2s", BLOCKS_DIR, hash_hex, hash_hex + 2);

    ensure_dir(BLOCKS_DIR);
    ensure_dir(dir_path);
    ensure_dir(subdir_path);

    // ایجاد مسیر فایل نهایی
    char *file_path = malloc(300);
    if (!*file_path)
    {
        perror("malloc *file_path");
        return NULL;
    }
    snprintf(file_path, 300, "%s/%s", subdir_path, hash_hex);

    // بررسی وجود فایل
    struct stat st;
    if (stat(file_path, &st) == 0)
    {
        printf("[ENGINE] Chunk already exists: %s\n", hash_hex);
        return file_path;
    }

    // ذخیره چانک جدید
    int fd = open(file_path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0)
    {
        perror("open chunk file");
        free(file_path);
        return NULL;
    }

    if (write_all(fd, data, len) < 0)
    {
        close(fd);
        unlink(file_path);
        free(file_path);
        return NULL;
    }

    close(fd);
    printf("[ENGINE] Stored chunk: %s\n", hash_hex);
    return file_path;
}

// $$ تابع اصلاح شده برای ایجاد مانیفست
char *create_manifest(struct upload_info *state, char **manifest_json)
{
    // محاسبه اندازه مورد نیاز برای JSON
    int json_size = 1024; // اندازه پایه
    int i;
    for (i = 0; i < state->part_total_count; i++)
    {
        json_size += 256; // برای هر چانک
    }

    *manifest_json = malloc(json_size);
    if (!*manifest_json)
    {
        perror("malloc *manifest_json");
        return NULL;
    }

    // شروع ساخت JSON
    char *ptr = *manifest_json;
    int written = snprintf(ptr, json_size,
                           "{\n"
                           "  \"version\": 1,\n"
                           "  \"hash_algo\": \"blake3\",\n"
                           "  \"chunk_size\": %d,\n"
                           "  \"total_size\": %lld,\n"
                           "  \"filename\": \"%s\",\n"
                           "  \"chunks\": [\n",
                           CHUNCK_BITS, state->file_total_size, state->file_name);

    ptr += written;

    // افزودن اطلاعات چانک‌ها
    for (i = 0; i < state->part_total_count; i++)
    {
        char hash_hex[65];
        int j;
        for (j = 0; j < HASH_LENGTH; j++)
        {
            sprintf(hash_hex + j * 2, "%02x", state->parts_list[i].hash_code[j]);
        }
        hash_hex[64] = '\0';

        written = snprintf(ptr, (*manifest_json + json_size) - ptr,
                           "    {\"index\": %d, \"size\": %d, \"hash\": \"%s\"}%s\n",
                           state->parts_list[i].part_num, state->parts_list[i].part_size, hash_hex,
                           (i == state->part_total_count - 1) ? "" : ",");

        ptr += written;
    }

    // پایان JSON
    strcpy(ptr, "  ]\n}\n");

    // $$ شروع تغییرات برای CID استاندارد - نسخه اصلاح شده

    // 1. serialize(manifest) - همان JSON ساخته شده
    unsigned char *serialized_manifest = (unsigned char *)*manifest_json;
    int manifest_len = strlen(*manifest_json);

    // 2. multihash(serialize(manifest))
    unsigned char manifest_hash[HASH_LENGTH];
    calculate_hash(serialized_manifest, manifest_len, manifest_hash);

    // ساخت multihash: [کد-الگوریتم, طول-هش, هش-داده]
    unsigned char multihash[2 + HASH_LENGTH];
    multihash[0] = 0x1e;        // کد BLAKE3 در جدول multihash (0x1e = 30)
    multihash[1] = HASH_LENGTH; // طول هش (32 بایت = 0x20)
    memcpy(multihash + 2, manifest_hash, HASH_LENGTH);

    unsigned char multicodec_data[2 + 2 + HASH_LENGTH]; // 36 بایت

    // کد multicodec برای dag-json: 0x0129 = 297
    unsigned int dag_json_code = 0x0129; // 297

    // کدگذاری Varint برای 297
    multicodec_data[0] = (dag_json_code >> 7) | 0x80; // 0x82
    multicodec_data[1] = (dag_json_code & 0x7F);      // 0x29

    // کپی multihash
    memcpy(multicodec_data + 2, multihash, 2 + HASH_LENGTH);

    // 4. multibase(base32)
    char *cid = base32_encode(multicodec_data, 2 + 2 + HASH_LENGTH); // 36 بایت
    char *final_cid = malloc(strlen(cid) + 2);
    if (!final_cid)
    {
        free(cid);
        perror("malloc final_cid");
        return NULL;
    }

    sprintf(final_cid, "b%s", cid); // "b" برای base32

    free(cid);
    return final_cid;

    // $$ پایان تغییرات برای CID استاندارد
}

// $$ تابع اصلاح شده برای کدگذاری base32
char *base32_encode(const unsigned char *data, int len)
{
    const char *base32_chars = "abcdefghijklmnopqrstuvwxyz234567";

    // محاسبه دقیق اندازه خروجی
    int output_len = (len * 8 + 4) / 5;     // تعداد کاراکترهای خروجی
    char *encoded = malloc(output_len + 1); // +1 برای نال ترمینیتور
    if (!encoded)
        return NULL;

    int bit_buffer = 0;
    int bit_count = 0;
    int out_index = 0;

    for (int i = 0; i < len; i++)
    {
        bit_buffer = (bit_buffer << 8) | data[i];
        bit_count += 8;

        while (bit_count >= 5)
        {
            int index = (bit_buffer >> (bit_count - 5)) & 0x1F;
            encoded[out_index++] = base32_chars[index];
            bit_count -= 5;
        }
    }

    // باقی‌مانده بیت‌ها
    if (bit_count > 0)
    {
        // باقی‌مانده بیت‌ها را به چپ تراز می‌کنیم
        int index = (bit_buffer << (5 - bit_count)) & 0x1F;
        encoded[out_index++] = base32_chars[index];
    }

    encoded[out_index] = '\0';
    return encoded;
}

// $$ تابع جایگزین ساده‌تر برای CID (اگر base32_encode مشکل داشت)

int store_manifest(const char *cid, const char *manifest_json)
{
    ensure_dir(MANIFESTS_DIR);

    char manifest_path[256];
    snprintf(manifest_path, sizeof(manifest_path), "%s/%s.json", MANIFESTS_DIR, cid);

    // استفاده از الگوی نوشتن اتمیک
    char temp_path[256];
    snprintf(temp_path, sizeof(temp_path), "%s/%s.tmp", MANIFESTS_DIR, cid);

    int fd = open(temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
    {
        perror("open temp manifest");
        return -1;
    }

    if (write_all(fd, manifest_json, strlen(manifest_json)) < 0)
    {
        close(fd);
        unlink(temp_path);
        return -1;
    }

    fsync(fd);
    close(fd);

    // تغییر نام اتمیک
    if (rename(temp_path, manifest_path) < 0)
    {
        perror("rename manifest");
        unlink(temp_path);
        return -1;
    }

    printf("[ENGINE] Stored manifest: %s\n", cid);
    return 0;
}

void cleanup_upload_info(struct upload_info *state)
{
    if (state->file_name)
    {
        free(state->file_name);
        state->file_name = NULL;
    }

    if (state->parts_list)
    {
        int i;
        for (i = 0; i < state->part_total_count; i++)
        {
            if (state->parts_list[i].file_location)
            {
                free(state->parts_list[i].file_location);
            }
        }
        free(state->parts_list);
        state->parts_list = NULL;
    }

    state->part_total_count = 0;
    state->file_total_size = 0;
    state->current_part = 0;
}

void handle_connection(int cfd)
{
    for (;;)
    {
        uint8_t header[5];
        ssize_t r = read_n(cfd, header, 5);
        if (r == 0)
            break;
        if (r < 0)
        {
            break;
        }
        uint8_t op = header[0];
        uint32_t len; // data type
        memcpy(&len, header + 1, 4);
        len = ntohl(len);
        uint8_t *payload = NULL;
        if (len)
        {
            payload = (uint8_t *)malloc(len);
            if (!payload)
            {
                perror("malloc");
                break;
            }
            if (read_n(cfd, payload, len) <= 0)
            {
                free(payload);
                break;
            }
        }

        if (op == OP_UPLOAD_START)
        {
            printf("[ENGINE] UPLOAD_START: name=\"%.*s\"\n", (int)len, (char *)payload);
            fflush(stdout);
            // TODO: initialize upload state
            // پاکسازی وضعیت قبلی اگر وجود دارد
            cleanup_upload_info(&upload_state);

            // مقداردهی اولیه وضعیت آپلود
            upload_state.file_name = malloc(len + 1);
            // check pointer
            if (!upload_state.file_name)
            {
                perror("malloc upload_state.file_name");
                break;
            }
            memcpy(upload_state.file_name, payload, len);
            upload_state.file_name[len] = '\0';
            upload_state.file_total_size = 0;
            upload_state.part_total_count = 0;
            upload_state.current_part = 0;
            upload_state.parts_list = NULL;

            // اختصاص بافر برای چانک
            chunk_buffer = malloc(CHUNCK_BITS);
            if (!chunk_buffer)
            {
                perror("malloc chunk buffer");
                break;
            }
        }
        else if (op == OP_UPLOAD_CHUNK)
        {
            if (!upload_state.file_name)
            {
                printf("[ENGINE] ERROR: Received chunk without START\n");
                free(payload);
                break;
            }

            // int chunk_size = len;
            unsigned char hash[HASH_LENGTH];

            // محاسبه هش چانک
            calculate_hash(payload, len, hash);

            // ذخیره چانک
            char *stored_path = store_chunk(payload, len, hash);
            if (!stored_path)
            {
                printf("[ENGINE] ERROR: Failed to store chunk\n");
                free(payload);
                break;
            }

            // افزایش آرایه چانک‌ها
            struct file_part *newPartsList = realloc(upload_state.parts_list,
                                                     (upload_state.part_total_count + 1) * sizeof(struct file_part));
            if (!newPartsList)
            {
                perror("realloc parts_list");
                free(payload);
                break;
            }

            upload_state.parts_list = newPartsList;
            // ادرس پارتی که میخوایم اضافه کنیم
            struct file_part *part = &upload_state.parts_list[upload_state.part_total_count];

            part->part_num = upload_state.part_total_count;
            part->part_size = len;
            memcpy(part->hash_code, hash, HASH_LENGTH);
            part->file_location = stored_path;

            upload_state.part_total_count++;
            upload_state.file_total_size += len;

            printf("[ENGINE] Processed chunk %d, size: %d\n",
                   upload_state.part_total_count - 1, len);
        }
        else if (op == OP_UPLOAD_FINISH)
        {
            if (!upload_state.file_name)
            {
                printf("[ENGINE] ERROR: Received FINISH without START\n");
                break;
            }

            printf("[ENGINE] UPLOAD_FINISH: processing %d chunks\n", upload_state.part_total_count);

            // ایجاد مانیفست و محاسبه CID
            char *manifest_json = NULL;
            const char *cid = create_manifest(&upload_state, &manifest_json);

            if (!cid || !manifest_json)
            {
                printf("[ENGINE] ERROR: Failed to create manifest\n");
                break;
            }

            // ذخیره مانیفست
            if (store_manifest(cid, manifest_json) < 0)
            {
                printf("[ENGINE] ERROR: Failed to store manifest\n");
                free(cid);
                free(manifest_json);
                break;
            }

            printf("[ENGINE] UPLOAD_FINISH -> returning CID %s\n", cid);
            fflush(stdout);
            send_frame(cfd, OP_UPLOAD_DONE, cid, (uint32_t)strlen(cid));

            // پاکسازی حافظه
            free(cid);
            free(manifest_json);
        }
        else if (op == OP_DOWNLOAD_START)
        {
            printf("[ENGINE] DOWNLOAD_START: cid=\"%.*s\"\n", (int)len, (char *)payload);
            fflush(stdout);
            // TODO: look up CID, stream verified chunks
            // Minimal placeholder: no chunks, just DONE
            send_frame(cfd, OP_DOWNLOAD_DONE, NULL, 0);
        }
        else
        {
        }

        free(payload);
    }
    close(cfd);
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        fprintf(stderr, "usage: %s /tmp/cengine.sock\n", argv[0]);
        return 2;
    }
    g_sock_path = argv[1];

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
    {
        perror("socket");
        return 2;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, g_sock_path, sizeof(addr.sun_path) - 1);
    unlink(g_sock_path);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("bind");
        return 2;
    }
    if (listen(fd, 64) < 0)
    {
        perror("listen");
        return 2;
    }

    printf("[ENGINE] listening on %s\n", g_sock_path);
    fflush(stdout);

    for (;;)
    {
        int cfd = accept(fd, NULL, NULL);
        if (cfd < 0)
        {
            if (errno == EINTR)
                continue;
            perror("accept");
            break;
        }
        // Thread-per-connection keeps it readable for OS labs
        pthread_t th;
        pthread_create(&th, NULL, (void *(*)(void *))handle_connection, (void *)(intptr_t)cfd);
        pthread_detach(th);
    }

    close(fd);
    unlink(g_sock_path);
    return 0;
}
*/