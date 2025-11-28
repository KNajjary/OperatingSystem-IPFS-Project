// Build: gcc -O2 -pthread -o c_engine c_engine.c
// Run:   ./c_engine /tmp/cengine.sock
// kkkk
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

#define DATA_BLOCK_SIZE (256 * 1024)
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
    struct stat st = {0};
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
    blake3_hasher_finalize(&hasher, hash, HASH_SIZE);
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
    if(!*file_path)
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
        uint32_t len; //data type
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
            chunk_buffer = malloc(CHUNK_SIZE);
            if (!chunk_buffer)
            {
                perror("malloc chunk buffer");
                break;
            }
        }
        else if (op == OP_UPLOAD_CHUNK)
        {
            // TODO: process chunk (hash/store); here just drop
        }
        else if (op == OP_UPLOAD_FINISH)
        {
            // TODO: finalize DAG and compute real CID
            const char *cid = "CID-PLACEHOLDER";
            printf("[ENGINE] UPLOAD_FINISH -> returning CID %s\n", cid);
            fflush(stdout);
            send_frame(cfd, OP_UPLOAD_DONE, cid, (uint32_t)strlen(cid));
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
