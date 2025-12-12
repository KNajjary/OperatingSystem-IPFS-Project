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
//@@down_ham
#include <pthread.h>
//@@down_ham

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

//@@down_ham
pthread_mutex_t manifest_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_rwlock_t blocks_rwlock = PTHREAD_RWLOCK_INITIALIZER;
pthread_mutex_t refcount_mutex = PTHREAD_MUTEX_INITIALIZER;
//@@down_ham

// $$ download
#define MAX_PATH_LENGTH 256
#define MAX_MANIFEST_SIZE (1024 * 1024) // 1MB

struct manifest_info
{
    char *filename;
    long long total_size;
    int chunk_count;
    struct file_part *chunks;
};
// $$ d0wnload

// @@down_ham
// ساختار برای reference counting بلاک‌ها(لیست پیوندی)
struct block_ref
{
    char hash_hex[65];
    int ref_count;
    struct block_ref *next;
};

struct block_ref *block_refs = NULL; // اولین عنصر لیست پیوندی
// @@down_ham

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
    struct stat st;
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

//$$download
// کل فایل را میخواند و بصورت یک رشته برمیگرداند
char *read_manifest_file(const char *cid)
{
    char manifest_path[MAX_PATH_LENGTH];
    snprintf(manifest_path, sizeof(manifest_path), "%s/%s.json", MANIFESTS_DIR, cid);

    FILE *file = fopen(manifest_path, "r");
    if (!file)
    {
        printf("[ENGINE] ERROR: Cannot open manifest file: %s\n", manifest_path);
        return NULL;
    }

    // پیدا کردن اندازه فایل
    fseek(file, 0, SEEK_END);     // put the pointer in end of file
    long file_size = ftell(file); // return current byte in file
    fseek(file, 0, SEEK_SET);     ////put the pointer in begining of file

    if (file_size > MAX_MANIFEST_SIZE)
    {
        printf("[ENGINE] ERROR: Manifest file too large: %ld\n", file_size);
        fclose(file);
        return NULL;
    }

    // خواندن کل فایل
    char *manifest_json = malloc(file_size + 1);
    if (!manifest_json)
    {
        fclose(file);
        return NULL;
    }

    size_t read_size = fread(manifest_json, 1, file_size, file); // size of each part=1 byte,number of parts
    manifest_json[read_size] = '\0';

    fclose(file);
    return manifest_json;
}

//  ذخیره اطلاعات رشته ورودی در استراکت manifest_info
int parse_manifest(const char *manifest_json, struct manifest_info *info)
{
    // این یک تجزیه‌کننده ساده JSON است
    const char *ptr = manifest_json; // اشاره گر برای پیمایش رشته

    // پیدا کردن filename
    const char *filename_start = strstr(ptr, "\"filename\":"); // جستجوی زیر رشته در رشته
    if (!filename_start)
        return -1;
    filename_start = strchr(filename_start, '"'); // جستجوی اولین وقوع کاراکتر"
    if (!filename_start)
        return -1;
    filename_start++; // عبور از "
    const char *filename_end = strchr(filename_start, '"');
    if (!filename_end)
        return -1;

    int filename_len = filename_end - filename_start;
    info->filename = malloc(filename_len + 1);
    memcpy(info->filename, filename_start, filename_len);
    info->filename[filename_len] = '\0';

    // پیدا کردن total_size
    const char *size_start = strstr(ptr, "\"total_size\":");
    if (!size_start)
        return -1;
    size_start += 12;                     // طول "\"total_size\":"
    info->total_size = atoll(size_start); // تبدیل رشته به عدد long long

    // شمردن تعداد چانک‌ها
    info->chunk_count = 0;
    const char *chunk_ptr = manifest_json;
    while ((chunk_ptr = strstr(chunk_ptr, "\"index\"")) != NULL)
    {
        info->chunk_count++;
        chunk_ptr++;
    }

    if (info->chunk_count == 0) // مانیفست بدون چانک معتبر نیست
    {
        return -1;
    }

    // اختصاص حافظه برای چانک‌ها
    info->chunks = malloc(info->chunk_count * sizeof(struct file_part));
    if (!info->chunks)
    {
        return -1;
    }

    // پر کردن اطلاعات چانک‌ها
    chunk_ptr = manifest_json;
    for (int i = 0; i < info->chunk_count; i++)
    {
        chunk_ptr = strstr(chunk_ptr, "\"index\":");
        if (!chunk_ptr)
            break;
        chunk_ptr += 7; // طول "\"index\":"
        info->chunks[i].part_num = atoi(chunk_ptr);

        chunk_ptr = strstr(chunk_ptr, "\"size\":");
        if (!chunk_ptr)
            break;
        chunk_ptr += 7;                              // طول "\"size\":"
        info->chunks[i].part_size = atoi(chunk_ptr); // تبدیل مقدار بعدی به عدد صحیح

        chunk_ptr = strstr(chunk_ptr, "\"hash\":");
        if (!chunk_ptr)
            break;
        chunk_ptr = strchr(chunk_ptr, '"');
        if (!chunk_ptr)
            break;
        chunk_ptr++; // عبور از "
        const char *hash_end = strchr(chunk_ptr, '"');
        if (!hash_end)
            break;

        // تبدیل هش هگزادسیمال به باینری
        char hash_hex[65];
        int hash_len = hash_end - chunk_ptr;
        if (hash_len != 64)
            break;

        memcpy(hash_hex, chunk_ptr, 64);
        hash_hex[64] = '\0';
        // تبدیل هگزادسیمال به باینری(64 به 32)
        for (int j = 0; j < HASH_LENGTH; j++)
        {
            sscanf(hash_hex + j * 2, "%2hhx", &info->chunks[i].hash_code[j]); // خواندن فرمت‌دار از رشته، خواندن دو کاراکتر هگز به عنوان یک بایت
        }

        // ساخت مسیر فایل از هش
        char *file_path = malloc(300);
        snprintf(file_path, 300, "%s/%.2s/%.2s/%s", BLOCKS_DIR, hash_hex, hash_hex + 2, hash_hex);
        info->chunks[i].file_location = file_path;

        chunk_ptr = hash_end + 1;
    }

    return 0;
}
// پاکسازی اطلاعات مانیفست
void cleanup_manifest_info(struct manifest_info *info)
{
    // فقط در صورتی که حافظه‌ای تخصیص داده شده باشد، آن را آزاد می‌کنیم
    if (info->filename)
    {
        free(info->filename);
        info->filename = NULL;
    }
    // فقط اگر آرایه چانک‌ها تخصیص داده شده باشد، آن را پردازش می‌کنیم
    if (info->chunks)
    {
        for (int i = 0; i < info->chunk_count; i++)
        {
            if (info->chunks[i].file_location)
            {
                free(info->chunks[i].file_location);
            }
        }
        free(info->chunks);
        info->chunks = NULL;
    }

    info->chunk_count = 0;
    info->total_size = 0;
}

// تابع برای راستی‌آزمایی چانک
int verify_chunk(const char *file_path, const unsigned char *expected_hash)
{
    FILE *file = fopen(file_path, "rb"); // read binary
    if (!file)
    {
        return -1;
    }

    // پیدا کردن اندازه فایل
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file); // تعداد کل بایت‌های فایل
    fseek(file, 0, SEEK_SET);

    // خواندن فایل
    unsigned char *file_data = malloc(file_size);
    if (!file_data)
    {
        fclose(file);
        return -1;
    }

    fread(file_data, 1, file_size, file);
    fclose(file);

    // محاسبه هش
    unsigned char actual_hash[HASH_LENGTH];
    calculate_hash(file_data, file_size, actual_hash);

    free(file_data);

    // مقایسه هش‌ها
    return memcmp(expected_hash, actual_hash, HASH_LENGTH) == 0 ? 0 : -1; // مقایسه دو بلوک حافظه
}
//$$download

//@@down_ham
// تابع برای افزایش reference count
int increment_ref_count(const char *hash_hex)
{
    pthread_mutex_lock(&refcount_mutex); // lock

    struct block_ref *current = block_refs; // اشاره گر به اول لیست پیوندی
    while (current != NULL)                 // تا اخر لیست
    {
        if (strcmp(current->hash_hex, hash_hex) == 0) //= یعنی این بلاک قبلاً در لیست وجود دارد
        {
            current->ref_count++;                  // یک دانلود دیگر از این بلاک در حال انجام است
            pthread_mutex_unlock(&refcount_mutex); // open lock
            return current->ref_count;
        }
        current = current->next;
    }

    // اگر بلاک پیدا نشد، جدید ایجاد کن
    struct block_ref *new_ref = malloc(sizeof(struct block_ref));
    if (!new_ref)
    {
        // ابتدا قفل را آزاد می‌کنیم و سپس مقدار -1 را به عنوان خطا برمی‌گردانیم
        pthread_mutex_unlock(&refcount_mutex);
        return -1;
    }
    // مقدار دهی عضو جدید لیست پیوندی
    strcpy(new_ref->hash_hex, hash_hex);
    new_ref->ref_count = 1;
    new_ref->next = block_refs;
    block_refs = new_ref; // اشاره به گره جدید. این باعث می‌شود گره جدید اولین گره لیست شود

    pthread_mutex_unlock(&refcount_mutex);
    return 1;
}

// تابع برای کاهش reference count
int decrement_ref_count(const char *hash_hex)
{
    pthread_mutex_lock(&refcount_mutex);

    struct block_ref **prev = &block_refs;  // double pointer
    struct block_ref *current = block_refs; // ابتدای لیست اشاره می‌کند

    while (current != NULL)
    {
        if (strcmp(current->hash_hex, hash_hex) == 0) //=
        {
            current->ref_count--;

            if (current->ref_count <= 0) // ین بلاک دیگر توسط هیچ دانلودی استفاده نمی‌شود
            {
                *prev = current->next; // delete node
                free(current);
            }

            pthread_mutex_unlock(&refcount_mutex);
            return 1;
        }
        prev = &(current->next);
        current = current->next;
    }

    pthread_mutex_unlock(&refcount_mutex);
    return -1;
}

//  تابع برای خواندن مانیفست با قفل‌گذاری
char *read_manifest_file_safe(const char *cid)
{
    pthread_mutex_lock(&manifest_mutex); //  قفل مانیفست

    char *manifest_json = read_manifest_file(cid);

    pthread_mutex_unlock(&manifest_mutex); //  آزاد کردن قفل
    return manifest_json;
}

// تابع برای راستی‌آزمایی چانک با قفل‌گذاری
int verify_chunk_safe(const char *file_path, const unsigned char *expected_hash)
{
    pthread_rwlock_rdlock(&blocks_rwlock); //  قفل خواندن بلاک‌ها

    int result = verify_chunk(file_path, expected_hash);

    pthread_rwlock_unlock(&blocks_rwlock); //  آزاد کردن قفل
    return result;
}

// تابع برای خواندن چانک با قفل‌گذاری
// file_size: اشاره‌گری به متغیر لانگ که اندازه فایل در آن ذخیره می‌شود را دریافت می‌کند و یک اشاره‌گر به داده‌های خوانده شده (آرایه بایت) برمی‌گرداند
unsigned char *read_chunk_file_safe(const char *file_path, long *file_size)
{
    pthread_rwlock_rdlock(&blocks_rwlock); //  قفل خواندن بلاک‌ها

    FILE *file = fopen(file_path, "rb");
    if (!file)
    {
        pthread_rwlock_unlock(&blocks_rwlock);
        return NULL;
    }

    fseek(file, 0, SEEK_END);
    *file_size = ftell(file);
    fseek(file, 0, SEEK_SET);

    unsigned char *file_data = malloc(*file_size);
    if (!file_data)
    {
        fclose(file);
        pthread_rwlock_unlock(&blocks_rwlock);
        return NULL;
    }

    fread(file_data, 1, *file_size, file);
    fclose(file);

    pthread_rwlock_unlock(&blocks_rwlock); // آزاد کردن قفل
    return file_data;
}

//  تابع برای پردازش دانلود همزمان
void handle_download(int cfd, const char *cid) // cfd سوکت برای ارتباط با کلاینت
{
    printf("[ENGINE] Starting download for CID: %s\n", cid);
    fflush(stdout);

    // خواندن مانیفست با قفل‌گذاری
    char *manifest_json = read_manifest_file_safe(cid);
    if (!manifest_json)
    {
        printf("[ENGINE] ERROR: Manifest not found for CID: %s\n", cid);
        return;
    }

    // تجزیه مانیفست
    struct manifest_info manifest;
    memset(&manifest, 0, sizeof(manifest)); // آماده‌سازی ساختار و صفر کردن بایت ها

    if (parse_manifest(manifest_json, &manifest) < 0)
    {
        printf("[ENGINE] ERROR: Failed to parse manifest\n");
        free(manifest_json);
        return;
    }

    printf("[ENGINE] Downloading file: %s, chunks: %d, total size: %lld\n",
           manifest.filename, manifest.chunk_count, manifest.total_size);

    //  افزایش reference count برای تمام بلاک‌ها
    for (int i = 0; i < manifest.chunk_count; i++)
    {
        char hash_hex[65];
        for (int j = 0; j < HASH_LENGTH; j++)
        {
            sprintf(hash_hex + j * 2, "%02x", manifest.chunks[i].hash_code[j]);
        }
        increment_ref_count(hash_hex);
    }

    // ارسال چانک‌ها به ترتیب
    for (int i = 0; i < manifest.chunk_count; i++)
    {
        struct file_part *chunk = &manifest.chunks[i]; // گرفتن اشاره‌گر به چانک فعلی

        // راستی‌آزمایی چانک با قفل‌گذاری
        if (verify_chunk_safe(chunk->file_location, chunk->hash_code) < 0)
        {
            printf("[ENGINE] WARNING: Chunk %d verification failed, skipping\n", chunk->part_num);
            continue;
        }

        // خواندن چانک با قفل‌گذاری
        long chunk_file_size;
        unsigned char *chunk_data = read_chunk_file_safe(chunk->file_location, &chunk_file_size);
        if (!chunk_data)
        {
            printf("[ENGINE] ERROR: Cannot read chunk file: %s\n", chunk->file_location);
            continue;
        }

        // ایجاد payload برای DOWNLOAD_CHUNK
        int header_size = 8;
        unsigned char *chunk_payload = malloc(header_size + chunk_file_size);
        if (!chunk_payload)
        {
            free(chunk_data);
            continue;
        }

        // پر کردن هدر
        uint32_t network_index = htonl(chunk->part_num);
        uint32_t network_size = htonl(chunk_file_size);

        memcpy(chunk_payload, &network_index, 4);
        memcpy(chunk_payload + 4, &network_size, 4);
        memcpy(chunk_payload + 8, chunk_data, chunk_file_size);

        // ارسال چانک
        if (send_frame(cfd, OP_DOWNLOAD_CHUNK, chunk_payload, header_size + chunk_file_size) < 0)
        {
            printf("[ENGINE] ERROR: Failed to send chunk %d\n", chunk->part_num);
        }
        else
        {
            printf("[ENGINE] Sent chunk %d, size: %ld\n", chunk->part_num, chunk_file_size);
        }

        free(chunk_data);
        free(chunk_payload);

        // تاخیر کوچک برای جلوگیری از overload
        usleep(1000);
    }

    // ارسال پایان دانلود
    printf("[ENGINE] Download completed for CID: %s\n", cid);
    send_frame(cfd, OP_DOWNLOAD_DONE, NULL, 0);

    //  کاهش reference count برای تمام بلاک‌ها
    for (int i = 0; i < manifest.chunk_count; i++)
    {
        char hash_hex[65];
        for (int j = 0; j < HASH_LENGTH; j++)
        {
            sprintf(hash_hex + j * 2, "%02x", manifest.chunks[i].hash_code[j]);
        }
        decrement_ref_count(hash_hex);
    }

    // پاکسازی
    cleanup_manifest_info(&manifest);
    free(manifest_json);
}

//@@down_ham

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

            // استخراج CID از payload
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

            // اجرای دانلود در thread جداگانه برای پشتیبانی از همزمانی
            handle_download(cfd, cid);

            free(cid);
            break;
        }
        else
        {
        }

        free(payload);
    }
    // پاکسازی
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