// Build: gcc -O2 -pthread -lblake3 -o c_engine c_engine.c
// Run:   ./c_engine /tmp/cengine.sock

#define _GNU_SOURCE
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <dirent.h>
#include <blake3.h>

#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
// #include <sys/stat.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
// #include <fcntl.h>
// #include <dirent.h>
#include <blake3.h>

#define OP_UPLOAD_START 0x01
#define OP_UPLOAD_CHUNK 0x02
#define OP_UPLOAD_FINISH 0x03
#define OP_UPLOAD_DONE 0x81

#define OP_DOWNLOAD_START 0x11
#define OP_DOWNLOAD_CHUNK 0x91
#define OP_DOWNLOAD_DONE 0x92

#define CHUNK_SIZE (256 * 1024)
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

// تابع برای ایجاد دایرکتوری اگر وجود ندارد
void ensure_dir(const char *path)
{
    struct stat st = {0};
    if (stat(path, &st) == -1)
    {
        mkdir(path, 0755);
    }
}

// تابع برای خواندن دقیق n بایت
int read_n(int fd, void *buf, int n)
{
    int got = 0;
    while (got < n)
    {
        int r = read(fd, (char *)buf + got, n - got);
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
    return got;
}

// تابع برای نوشتن کامل بافر
int write_all(int fd, const void *buf, int n)
{
    int sent = 0;
    while (sent < n)
    {
        int w = write(fd, (const char *)buf + sent, n - sent);
        if (w < 0)
        {
            if (errno == EINTR)
                continue;
            perror("write");
            return -1;
        }
        sent += w;
    }
    return 0;
}

// تابع برای ارسال فریم
int send_frame(int fd, unsigned char op, const void *payload, unsigned int len)
{
    unsigned char header[5];
    header[0] = op;
    unsigned int be_len = htonl(len);
    memcpy(header + 1, &be_len, 4);
    if (write_all(fd, header, 5) < 0)
        return -1;
    if (len && write_all(fd, payload, len) < 0)
        return -1;
    return 0;
}

// تابع برای محاسبه هش BLAKE3
void calculate_hash(const unsigned char *data, int len, unsigned char *hash)
{
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data, len);
    blake3_hasher_finalize(&hasher, hash, HASH_LENGTH);
}

// تابع برای ذخیره چانک در سیستم فایل
char *store_chunk(const unsigned char *data, int len, const unsigned char *hash)
{
    // تبدیل هش به رشته هگزادسیمال
    char hash_hex[65];
    int i;
    for (i = 0; i < HASH_LENGTH; i++)
    {
        sprintf(hash_hex + i * 2, "%02x", hash[i]);
    }
    hash_hex[64] = '\0';

    // ایجاد ساختار دایرکتوری بر اساس دو کاراکتر اول و دوم هش
    char dir_path[256];
    char subdir_path[256];
    snprintf(dir_path, sizeof(dir_path), "%s/%.2s", BLOCKS_DIR, hash_hex);
    snprintf(subdir_path, sizeof(subdir_path), "%s/%.2s/%.2s", BLOCKS_DIR, hash_hex, hash_hex + 2);

    ensure_dir(BLOCKS_DIR);
    ensure_dir(dir_path);
    ensure_dir(subdir_path);

    // ایجاد مسیر فایل نهایی
    char *file_path = malloc(300);
    snprintf(file_path, 300, "%s/%s", subdir_path, hash_hex);

    // بررسی وجود فایل (deduplication)
    struct stat st;
    if (stat(file_path, &st) == 0)
    {
        // فایل از قبل وجود دارد
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

// تابع برای ایجاد مانیفست
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
        return NULL;

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
                           CHUNK_SIZE, state->file_total_size, state->file_name);

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

    // محاسبه هش مانیفست (CID)
    unsigned char manifest_hash[HASH_LENGTH];
    calculate_hash((unsigned char *)*manifest_json, strlen(*manifest_json), manifest_hash);

    // تبدیل به رشته هگزادسیمال برای CID
    char *cid = malloc(65);
    for (i = 0; i < HASH_LENGTH; i++)
    {
        sprintf(cid + i * 2, "%02x", manifest_hash[i]);
    }
    cid[64] = '\0';

    return cid;
}

// تابع برای ذخیره مانیفست
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

// تابع برای پاکسازی وضعیت آپلود
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

// تابع اصلی برای مدیریت اتصال
void handle_connection(int cfd)
{
    struct upload_info upload_state = {0};
    unsigned char *chunk_buffer = NULL;

    while (1)
    {
        unsigned char header[5];
        int r = read_n(cfd, header, 5);
        if (r == 0)
            break;
        if (r < 0)
        {
            break;
        }

        unsigned char op = header[0];
        unsigned int len;
        memcpy(&len, header + 1, 4);
        len = ntohl(len);

        unsigned char *payload = NULL;
        if (len)
        {
            payload = (unsigned char *)malloc(len);
            if (!payload)
            {
                perror("malloc payload");
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

            // پاکسازی وضعیت قبلی اگر وجود دارد
            cleanup_upload_info(&upload_state);

            // مقداردهی اولیه وضعیت آپلود
            upload_state.file_name = malloc(len + 1);
            memcpy(upload_state.file_name, payload, len);
            upload_state.file_name[len] = '\0';
            upload_state.file_total_size = 0;
            upload_state.part_total_count = 0;
            upload_state.current_part = 0;
            upload_state.parts_list = NULL;

            // اختصاص بافر برای چانک
            chunk_buffer = malloc(CHUNK_SIZE);
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

            // پردازش چانک
            int chunk_size = len;
            unsigned char hash[HASH_LENGTH];

            // محاسبه هش چانک
            calculate_hash(payload, chunk_size, hash);

            // ذخیره چانک
            char *stored_path = store_chunk(payload, chunk_size, hash);
            if (!stored_path)
            {
                printf("[ENGINE] ERROR: Failed to store chunk\n");
                free(payload);
                break;
            }

            // افزایش آرایه چانک‌ها
            struct file_part *new_parts = realloc(upload_state.parts_list,
                                                  (upload_state.part_total_count + 1) * sizeof(struct file_part));
            if (!new_parts)
            {
                perror("realloc parts_list");
                free(payload);
                break;
            }

            upload_state.parts_list = new_parts;
            struct file_part *part = &upload_state.parts_list[upload_state.part_total_count];

            part->part_num = upload_state.part_total_count;
            part->part_size = chunk_size;
            memcpy(part->hash_code, hash, HASH_LENGTH);
            part->file_location = stored_path;

            upload_state.part_total_count++;
            upload_state.file_total_size += chunk_size;

            printf("[ENGINE] Processed chunk %d, size: %d\n",
                   upload_state.part_total_count - 1, chunk_size);
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
            char *cid = create_manifest(&upload_state, &manifest_json);

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

            // ارسال پاسخ به کلاینت
            send_frame(cfd, OP_UPLOAD_DONE, cid, (unsigned int)strlen(cid));

            // پاکسازی حافظه
            free(cid);
            free(manifest_json);
        }
        else if (op == OP_DOWNLOAD_START)
        {
            printf("[ENGINE] DOWNLOAD_START: cid=\"%.*s\"\n", (int)len, (char *)payload);
            // TODO: پیاده‌سازی دانلود در فاز بعدی
            send_frame(cfd, OP_DOWNLOAD_DONE, NULL, 0);
        }
        else
        {
            printf("[ENGINE] Unknown opcode: 0x%02x\n", op);
        }

        free(payload);

        // اگر آپلود تمام شده، از حلقه خارج شو
        if (op == OP_UPLOAD_FINISH)
        {
            break;
        }
    }

    // پاکسازی حافظه
    cleanup_upload_info(&upload_state);
    if (chunk_buffer)
        free(chunk_buffer);

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

    // ایجاد دایرکتوری‌های مورد نیاز
    ensure_dir(BLOCKS_DIR);
    ensure_dir(MANIFESTS_DIR);

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

    // حذف فایل سوکت قبلی اگر وجود دارد
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

    while (1)
    {
        int cfd = accept(fd, NULL, NULL);
        if (cfd < 0)
        {
            if (errno == EINTR)
                continue;
            perror("accept");
            break;
        }

        // اتصال هر کلاینت در یک thread جداگانه
        pthread_t th;
        pthread_create(&th, NULL, (void *(*)(void *))handle_connection, (void *)(long)cfd);
        pthread_detach(th);
    }

    close(fd);
    unlink(g_sock_path);
    return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
char *create_simple_cid(struct upload_info *state, char **manifest_json)
{
    // ابتدا مانیفست را می‌سازیم (کد قبلی)
    // ... (کد ساخت مانیفست مانند قبل)

    // محاسبه هش مانیفست
    unsigned char manifest_hash[HASH_LENGTH];
    calculate_hash((unsigned char *)*manifest_json, strlen(*manifest_json), manifest_hash);

    // ساخت CID ساده‌تر با فرمت: base16(multihash)
    // "f0" + "20" + hash_hex (f0 = blake3, 20 = 32 بایت)
    char *cid = malloc(4 + 64 + 1); // "f0" + "20" + 64 کاراکتر هگز + نال ترمینیتور
    if (!cid)
        return NULL;

    sprintf(cid, "f020"); // f0=blake3 (0x1e), 20=32 بایت
    for (int i = 0; i < HASH_LENGTH; i++)
    {
        sprintf(cid + 4 + i * 2, "%02x", manifest_hash[i]);
    }

    return cid;
}

