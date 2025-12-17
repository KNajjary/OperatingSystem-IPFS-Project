// Build: gcc -O2 -pthread -o c_engine c_engine.c -w -n
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
//#include <blake3.h>
//#include "blake3/blake3.h"
//#include <openssl/sha.h>
#include <time.h>
/*#include "download.c"
#include "upload.c"'
#include "thread_pool_managment.c"
#include "locks_managment.c"*/
//#include "structs.c"
// callback
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


// ==================== متغیرهای سراسری ====================
static const char *g_sock_path = NULL;

//#############################################################################
//#############################################################################
//########################structs.c################################################
//#############################################################################
//#############################################################################
// structs.c

typedef struct download_state download_state_t;
typedef struct chunk_entry chunk_entry_t;
typedef struct manifest_entry manifest_entry_t;
typedef struct manifest_info manifest_info_t;
typedef struct manifest_cache manifest_cache_t;
typedef struct sequential_merger sequential_merger_t;
typedef struct manifest_lock_entry manifest_lock_entry_t;
typedef struct download_state download_state_t;
//typedef struct

static const char *BLOCKS_DIR = "blocks";
static const char *MANIFESTS_DIR = "manifests";

// جدول مانیفست‌ها
static manifest_entry_t **manifest_table = NULL;
static int manifest_table_size = 0;
static pthread_mutex_t manifest_table_lock = PTHREAD_MUTEX_INITIALIZER;

// جدول هش برای چانک‌ها (ساده‌شده)
static chunk_entry_t **chunk_table = NULL;
static int chunk_table_size = 0;
static pthread_mutex_t chunk_table_lock = PTHREAD_MUTEX_INITIALIZER;



// $$ download
#define MAX_PATH_LENGTH 256
#define MAX_MANIFEST_SIZE (1024 * 1024) // 1MB

#define CID_MAX_LEN 128

#define CHUNK_SIZE         (256 * 1024)  // 256KB
#define HASH_LENGTH        32
#define MAX_THREADS        8
#define MAX_TASKS          100
#define CID_PREFIX         "b"

#define HASH_ALGO "blake3"

#define LOCK_TABLE_SIZE 64  // اندازه جدول هش

#define LOCK_SUCCESS       0
#define LOCK_FAILED       -1  // خطای عمومی
#define LOCK_TIMEOUT      -2  // timeout رسیده
#define LOCK_NO_MEMORY    -3  // حافظه کافی نیست

// ==================== ساختارهای داده برای مدیریت قفل====================

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


// ساختار برای ذخیره اطلاعات هر چانک
chunk_entry_t {
    char hash_hex[65];          // هش هگزادسیمال
    char *file_path;            // مسیر فایل
    int ref_count;              // شمارنده مرجع
    pthread_mutex_t lock;       // قفل برای این چانک
    time_t last_access;         // زمان آخرین دسترسی
} ;

// ساختار برای ذخیره اطلاعات مانیفست
/*manifest_entry_t{
    char *cid;                  // CID
    char *manifest_path;        // مسیر فایل مانیفست
    int reader_count;           // تعداد خوانندگان همزمان
    pthread_rwlock_t rwlock;    // قفل خواننده/نویسنده
} ;*/
/**
 * ساختار entry برای مانیفست‌ها (اگر ندارید باید تعریف کنید)
 */

 manifest_entry_t {
    char cid[128];                  // شناسه محتوا
    manifest_info_t *manifest_info; // اطلاعات مانیفست (اشاره‌گر به داده‌های تجزیه شده)
    int ref_count;                  // شمارنده ارجاع
    time_t last_access;            // زمان آخرین دسترسی
    pthread_mutex_t lock;          // قفل مخصوص این entry
    int marked_for_deletion;       // علامت برای حذف
} ;

// 1. ساختار اطلاعات مانیفست با قفل خواننده/نویسنده
/*manifest_info_t {
    char *cid;                      // CID (مثلاً "bciq2k...")
    char *manifest_json;            // محتوای JSON مانیفست
    int ref_count;                  // تعداد خوانندگان فعال همزمان
    pthread_rwlock_t rwlock;        // قفل خواننده/نویسنده برای دسترسی همزمان
    time_t last_access;             // زمان آخرین دسترسی (برای LRU cache)
    struct manifest_info *next;     // برای لیست پیوندی/هش‌تگل
    int chunk_count;                // تعداد چانک‌ها در این مانیفست
    char ** chunk_hashes;            // آرایه هش‌های چانک‌ها
    int * chunk_sizes;

    char *filename;
    long long total_size;
    //struct file_part *chunks;               // آرایه اندازه‌های چانک‌ها
} ;*/
manifest_info_t
{
    char *filename;
    long long total_size;
    //int chunk_count;
    struct file_part *chunks;
};
// 5. ساختار برای ذخیره کَش مانیفست‌ها
manifest_cache_t{
    manifest_info_t **table;        // جدول هش برای مانیفست‌ها
    int size;                       // اندازه جدول
    int capacity;                   // ظرفیت جدول
    pthread_mutex_t lock;           // قفل جدول
    int max_capacity;               // حداکثر تعداد مانیفست‌ها در کَش
    manifest_info_t *lru_head;      // برای الگوریتم LRU
    manifest_info_t *lru_tail;      // برای الگوریتم LRU
} ;

// 6. ساختار برای ادغام‌کننده ترتیبی (Sequential Merger)
sequential_merger_t{
    download_state_t *dl_state;     // وضعیت دانلود مربوطه
    int window_size;                // اندازه پنجره برای ارسال
    int *window;                    // پنجره چانک‌های در حال ارسال
    int window_start;               // شروع پنجره
    int window_end;                 // پایان پنجره
    pthread_mutex_t lock;           // قفل ادغام‌کننده
    pthread_cond_t window_cond;     // شرط تغییر پنجره
} ;

// ساختار entry برای قفل
manifest_lock_entry_t {
    char cid[CID_MAX_LEN];
    pthread_rwlock_t rwlock;          // قفل خواندن/نوشتن برای این CID
    int ref_count;                    // شمارنده استفاده‌کنندگان
    struct manifest_lock_entry *next; // برای پیوند لیست
} ;

// ==================== ساختارهای داده برای دانلود همزمان ====================

// 2. ساختار وضعیت دانلود برای هر اتصال کلاینت
download_state_t {
    char *cid;                      // CID درخواستی
    int client_fd;                  // فایل‌دسکریپتور سوکت کلاینت
    manifest_info_t *manifest;      // اشاره‌گر به مانیفست مربوطه
    
    // اطلاعات چانک‌ها
    int total_chunks;               // تعداد کل چانک‌ها
    int *chunk_order;               // آرایه ترتیب ارسال چانک‌ها
    uint8_t **chunk_data;           // آرایه داده‌های چانک‌ها (اشاره‌گر)
    size_t * chunk_sizes;            // آرایه اندازه هر چانک
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
} ;

/*// وضعیت دانلود برای هر اتصال
typedef struct {
    char *cid;                      // CID
    manifest_entry_t *manifest;     // اشاره‌گر به مانیفست
    int total_chunks;               // تعداد کل چانک‌ها
    int chunks_sent;                // چانک‌های ارسال شده
    pthread_mutex_t lock;           // قفل وضعیت
} download_state_t;*/





//#############################################################################
//#############################################################################
//############################## locks_managment.c ###############################################
//#############################################################################
//#############################################################################

// جدول هش برای مدیریت قفل‌ها
typedef struct {
    manifest_lock_entry_t * locks[LOCK_TABLE_SIZE];
    pthread_mutex_t table_lock;       // قفل برای دسترسی به جدول
} manifest_lock_table_t;


// متغیر سراسری جدول قفل‌ها
static manifest_lock_table_t manifest_lock_table;

// تابع هش برای CID=============could be the same as calculate hash
static unsigned int hash_cid(const char *cid) {
    unsigned int hash = 5381;
    int c;
    
    while ((c = *cid++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    
    return hash;
}
// ==================== مدیریت مانیفست‌ها ====================
// مقداردهی اولیه سیستم قفل‌ها
int manifest_lock_init(void) {
    // مقداردهی اولیه جدول هش
    memset(&g_lock_table, 0, sizeof(g_lock_table));
    
    // مقداردهی اولیه قفل جدول
    if (pthread_mutex_init(&g_lock_table.table_lock, NULL) != 0) {
        return -1;
    }
    
    // مقداردهی اولیه همه locks به NULL
    for (int i = 0; i < LOCK_TABLE_SIZE; i++) {
        g_lock_table.locks[i] = NULL;
    }
    g_lock_table.table_lock = PTHREAD_MUTEX_INITIALIZER;
    
    return 0;
}

// تخریب سیستم قفل‌ها
int manifest_lock_destroy(void) {
    // پاک‌سازی همه قفل‌ها
    for (int i = 0; i < LOCK_TABLE_SIZE; i++) {
        manifest_lock_entry_t *entry = g_lock_table.lock[i];
        while (entry != NULL) {
            manifest_lock_entry_t *next = entry->next;
            
            // تخریب rwlock
            pthread_rwlock_destroy(&entry->rwlock);
            
            // تخریب قفل سراسری اگر نیاز بود
            // (اما بهتر است جداگانه مدیریت شود)
            
            free(entry);
            entry = next;
        }
        g_lock_table.locks[i] = NULL;
    }
    
    // تخریب قفل جدول
    pthread_mutex_destroy(&g_lock_table.table_lock);
    
    return 0;
}

// پیدا کردن یا ایجاد entry برای یک CID
static manifest_lock_entry_t* get_or_create_lock_entry(const char *cid) {
    manifest_lock_entry_t *entry, *new_entry, **prev_next;
    unsigned int bucket;
    
    bucket = hash_cid(cid) % LOCK_TABLE_SIZE;
    
    // قفل جدول برای جستجو/درج
    pthread_mutex_lock(&g_lock_table.table_lock);
    
    // جستجو در لیست زنجیره‌ای
    prev_next = &g_lock_table.locks[bucket];
    entry = g_lock_table.locks[bucket];
    
    while (entry != NULL) {
        if (strcmp(entry->cid, cid) == 0) {
            // CID پیدا شد - افزایش ref_count
            entry->ref_count++;
            pthread_mutex_unlock(&g_lock_table.table_lock);
            return entry;
        }
        prev_next = &entry->next;
        entry = entry->next;
    }
    
    // CID پیدا نشد - ایجاد entry جدید
    new_entry = malloc(sizeof(manifest_lock_entry_t));
    if (new_entry == NULL) {
        pthread_mutex_unlock(&g_lock_table.table_lock);
        return NULL;
    }
    
    // مقداردهی اولیه entry جدید
    strncpy(new_entry->cid, cid, CID_MAX_LEN - 1);
    new_entry->cid[CID_MAX_LEN - 1] = '\0';
    
    if (pthread_rwlock_init(&new_entry->rwlock, NULL) != 0) {
        free(new_entry);
        pthread_mutex_unlock(&g_lock_table.table_lock);
        return NULL;
    }
    
    new_entry->ref_count = 1;
    new_entry->next = NULL;
    
    // درج در لیست
    *prev_next = new_entry;
    
    pthread_mutex_unlock(&g_lock_table.table_lock);
    return new_entry;
}


// آزاد کردن قفل و کاهش ref_count
static int decrement_ref_count(const char *cid) {
    unsigned int bucket;
    manifest_lock_entry_t *entry, *prev;
    
    bucket = hash_cid(cid) % LOCK_TABLE_SIZE;
    
    pthread_mutex_lock(&g_lock_table.table_lock);
    
    // جستجو در لیست
    entry = g_lock_table.locks[bucket];
    prev = NULL;
    
    while (entry != NULL && strcmp(entry->cid, cid) != 0) {
        prev = entry;
        entry = entry->next;
    }
    
    if (entry == NULL) {
        pthread_mutex_unlock(&g_lock_table.table_lock);
        return -1; // CID پیدا نشد
    }
    
    // کاهش ref_count
    entry->ref_count--;
    
    // اگر ref_count به صفر رسید، حذف entry
    if (entry->ref_count == 0) {
        if (prev == NULL) {
            g_lock_table.locks[bucket] = entry->next;
        } else {
            prev->next = entry->next;
        }
        
        pthread_mutex_unlock(&g_lock_table.table_lock);
        
        // تخریب منابع
        pthread_rwlock_destroy(&entry->rwlock);
        free(entry);
    } else {
        pthread_mutex_unlock(&g_lock_table.table_lock);
    }
    
    return 0;
}

// آزاد کردن قفل خواندن/نوشتن
int manifest_unlock(const char *cid, int is_writer) {
    unsigned int bucket;
    manifest_lock_entry_t *entry;
    
    bucket = hash_cid(cid) % LOCK_TABLE_SIZE;
    
    // ابتدا قفل مناسب را آزاد کنید
    pthread_mutex_lock(&g_lock_table.table_lock);
    
    // جستجو در لیست (بدون حذف)
    entry = g_lock_table.locks[bucket];
    while (entry != NULL && strcmp(entry->cid, cid) != 0) {
        entry = entry->next;
    }
    
    if (entry == NULL) {
        pthread_mutex_unlock(&g_lock_table.table_lock);
        return -1; // CID پیدا نشد
    }
    
    // آزاد کردن قفل rwlock
    pthread_mutex_unlock(&g_lock_table.table_lock); // آزاد کردن قفل جدول اول
    
    if (pthread_rwlock_unlock(&entry->rwlock) != 0) {
        return -1;
    }
    
    // کاهش ref_count
    return decrement_ref_count(cid);
}



// گرفتن قفل خواندن برای یک CID
int manifest_read_lock(const char *cid) {
    manifest_lock_entry_t *entry;
    
    // پیدا کردن یا ایجاد entry
    entry = get_or_create_lock_entry(cid);
    if (entry == NULL) {
        return LOCK_NO_MEMORY; // حافظه کافی نیست
    }
    
    // گرفتن قفل خواندن (می‌تواند مسدود شود)
    if (pthread_rwlock_rdlock(&entry->rwlock) != 0) {
        // در صورت شکست، ref_count را کاهش دهید
        pthread_mutex_lock(&g_lock_table.table_lock);
        entry->ref_count--;
        pthread_mutex_unlock(&g_lock_table.table_lock);
        return LOCK_FAILED;
    }
    
    return LOCK_SUCCESS;
}

// گرفتن قفل نوشتن برای یک CID
int manifest_write_lock(const char *cid) {
    manifest_lock_entry_t *entry;
    
    // پیدا کردن یا ایجاد entry
    entry = get_or_create_lock_entry(cid);
    if (entry == NULL) {
        return LOCK_NO_MEMORY; // حافظه کافی نیست
    }
    
    // گرفتن قفل نوشتن (انحصاری)
    if (pthread_rwlock_wrlock(&entry->rwlock) != 0) {
        // در صورت شکست، ref_count را کاهش دهید
        pthread_mutex_lock(&g_lock_table.table_lock);
        entry->ref_count--;
        pthread_mutex_unlock(&g_lock_table.table_lock);
        return LOCK_FAILED;
    }
    
    return LOCK_SUCCESS;
}


// تابع trylock برای جلوگیری از deadlock
// نسخه بهبود یافته با مدیریت خطای بهتر
// کدهای بازگشتی تابع
/*
#define LOCK_SUCCESS       0
#define LOCK_FAILED       -1  // خطای عمومی
#define LOCK_TIMEOUT      -2  // timeout رسیده
#define LOCK_NO_MEMORY    -3  // حافظه کافی نیست
*/
int manifest_write_trylock(const char *cid, int timeout_seconds) {
    manifest_lock_entry_t *entry;
    struct timespec timeout;
    int result;
    
    if (timeout_seconds < 0) {
        return LOCK_FAILED; // timeout نامعتبر
    }
    
    // پیدا کردن یا ایجاد entry
    entry = get_or_create_lock_entry(cid);
    if (entry == NULL) {
        return LOCK_NO_MEMORY;
    }
    
    // تنظیم timeout
    if (clock_gettime(CLOCK_REALTIME, &timeout) != 0) {
        decrement_ref_count(cid);
        return LOCK_FAILED;
    }
    timeout.tv_sec += timeout_seconds;
    
    // تلاش برای گرفتن قفل با timeout
    result = pthread_rwlock_timedwrlock(&entry->rwlock, &timeout);
    
    if (result == ETIMEDOUT) {
        // Timeout رسیده
        decrement_ref_count(cid);
        return LOCK_TIMEOUT;
    } else if (result != 0) {
        // خطای دیگر
        decrement_ref_count(cid);
        return LOCK_FAILED;
    }
    
    // موفقیت‌آمیز
    return LOCK_SUCCESS;
}
//#############################################################################
//#############################################################################
//################################ thread_pool_managment.c #############################################
//#############################################################################
//#############################################################################
/*
در Thread Pool
 تعدادی نخ از قبل ایجاد شده‌اند و 
وظایف  در یک صف قرار می‌گیرند. 
هر نخ که آزاد باشد، یک وظیفه از صف برمی‌دارد 
و اجرا می‌کند
 = task
*/
/*// برای آپلود:
task_t *upload_task = create_upload_chunk_task(state, index, data, size, callback, arg);
thread_pool_submit_task(upload_task);

// برای دانلود:
task_t *download_task = create_download_chunk_task(state, index, hash, path, size, callback, arg);
thread_pool_submit_task(download_task);
*/


@@
typedef struct {
    download_state_t *state;
    int chunk_index;
} download_chunk_cb_arg_t;

@@
void chunk_download_callback(void *arg, int result) {
    download_chunk_cb_arg_t *cb = (download_chunk_cb_arg_t*)arg;
    download_state_t *state = cb->state;
    int chunk_index = cb->chunk_index;

    pthread_mutex_lock(&state->lock);

    if (result == 0) {
        state->chunk_ready[chunk_index] = 1;
        state->chunks_ready_count++;

        if (chunk_index == state->next_expected_index) {
            pthread_cond_signal(&state->chunk_ready_cond);
        }

        if (state->chunks_ready_count == state->total_chunks) {
            pthread_cond_broadcast(&state->chunk_ready_cond);
        }
    } else {
        state->chunk_ready[chunk_index] = -1;
    }

    pthread_mutex_unlock(&state->lock);

    free(cb);
}
//download_start
// ==================== ساختارهای اصلی ====================

// وضعیت دانلود (Download State)
// این ساختار برای پیگیری وضعیت یک عملیات دانلود خاص استفاده می‌شود
// هر درخواست دانلود یک نمونه از این ساختار خواهد داشت
typedef struct download_state {
    char cid[128];                      // شناسه محتوای (Content ID) فایل مورد نظر برای دانلود
                                        // CID یک رشته یکتا است که فایل را شناسایی می‌کند
    
    int chunk_count;                    // تعداد کل چانک‌های فایل
                                        // این عدد از فایل مانیفست خوانده می‌شود
    
    int chunks_processed;               // تعداد چانک‌هایی که تاکنون پردازش شده‌اند
                                        // وقتی این عدد برابر chunk_count شود، دانلود کامل شده است
    
    pthread_mutex_t lock;               // قفل (Mutex) برای دسترسی thread-safe به این ساختار
                                        // از race condition بین نخ‌های مختلف جلوگیری می‌کند
    
    pthread_cond_t all_chunks_ready;    // متغیر شرطی (Condition Variable) برای منتظر ماندن
                                        // نخ‌ها منتظر این سیگنال می‌مانند تا همه چانک‌ها آماده شوند
    
    struct download_chunk *chunks;      // اشاره‌گر به آرایه‌ای از ساختارهای download_chunk
                                        // هر عنصر این آرایه اطلاعات یک چانک را نگه می‌دارد
                                        // این آرایه به صورت پویا (dynamic) تخصیص داده می‌شود
} download_state_t;

// وضعیت آپلود (Upload State)
// این ساختار برای پیگیری وضعیت یک عملیات آپلود خاص استفاده می‌شود
// هر درخواست آپلود یک نمونه از این ساختار خواهد داشت
typedef struct upload_state {
    int client_fd;                      // فایل توصیف‌گر (File Descriptor) سوکت کلاینت
                                        // برای ارسال پاسخ به کلاینت استفاده می‌شود
    
    char filename[256];                 // نام اصلی فایل که توسط کاربر ارسال شده است
                                        // برای ذخیره در متادیتا استفاده می‌شود
    
    int chunk_count;                    // تعداد کل چانک‌های فایل
                                        // بر اساس اندازه فایل و اندازه استاندارد چانک محاسبه می‌شود
    
    int chunks_processed;               // تعداد چانک‌هایی که تاکنون پردازش شده‌اند
                                        // وقتی این عدد برابر chunk_count شود، آپلود کامل شده است
    
    pthread_mutex_t lock;               // قفل (Mutex) برای دسترسی thread-safe به این ساختار
                                        // از تداخل دسترسی نخ‌های مختلف جلوگیری می‌کند
    
    pthread_cond_t all_chunks_processed; // متغیر شرطی (Condition Variable) برای منتظر ماندن
                                         // وقتی همه چانک‌ها پردازش شدند، این سیگنال ارسال می‌شود
    
    struct upload_chunk *chunks;        // اشاره‌گر به آرایه‌ای از ساختارهای upload_chunk
                                        // هر عنصر این آرایه اطلاعات یک چانک آپلود شده را نگه می‌دارد
                                        // این آرایه به صورت پویا تخصیص داده می‌شود
} upload_state_t;

// انواع وظایف (Task Types)
// این enum انواع مختلف وظایفی که می‌توانند در Thread Pool اجرا شوند را تعریف می‌کند
// استفاده از enum به جای اعداد جادویی (magic numbers) کد را خوانا‌تر می‌کند
typedef enum {
    TASK_TYPE_UPLOAD_CHUNK,      // وظیفه پردازش یک چانک آپلود
                                 // شامل محاسبه هش، ذخیره فایل و به‌روزرسانی وضعیت
    
    TASK_TYPE_DOWNLOAD_CHUNK,    // وظیفه پردازش یک چانک دانلود
                                 // شامل خواندن از دیسک، تأیید هش و بارگذاری در حافظه
    
    TASK_TYPE_COMPUTE_HASH,      // وظیفه محاسبه هش (برای استفاده‌های عمومی)
                                 // می‌تواند برای عملیات‌های دیگر هم استفاده شود
    
    TASK_TYPE_WRITE_FILE         // وظیفه نوشتن فایل (برای استفاده‌های عمومی)
                                 // می‌تواند برای عملیات‌های I/O دیگر استفاده شود
    
    // مثال: TASK_TYPE_DELETE_FILE, TASK_TYPE_VALIDATE_CHUNK
} task_type_t;

// ساختار یکپارچه وظیفه (Unified Task Structure)
// این ساختار همه انواع وظایف را در یک فرمت یکپارچه نگه می‌دارد
// استفاده از union باعث صرفه‌جویی در حافظه می‌شود چون فقط داده‌های مربوط به نوع وظیفه ذخیره می‌شوند
typedef struct {
    task_type_t type;            // نوع وظیفه (از enum بالا)
                                 // تعیین می‌کند کدام بخش union استفاده شود
    
    union {                      // union - فقط یکی از این ساختارها در هر زمان استفاده می‌شود
                                 // اندازه union برابر بزرگترین عضو آن است
        
        struct {                 // ساختار داده برای وظایف آپلود
            upload_state_t *upload_state;  // اشاره‌گر به وضعیت آپلود مربوطه
            int chunk_index;               // اندیس چانک (۰-بنیاد) در فایل
            uint8_t *data;                 // اشاره‌گر به داده‌های خام چانک
            size_t data_size;              // اندازه داده‌ها به بایت
        } upload;
        
        struct {                 // ساختار داده برای وظایف دانلود
            download_state_t *download_state;  // اشاره‌گر به وضعیت دانلود مربوطه
            int chunk_index;                   // اندیس چانک (۰-بنیاد) در فایل
            char chunk_hash[65];               // هش مورد انتظار چانک (رشته hex 64 کاراکتری + NULL)
            char chunk_path[512];              // مسیر کامل فایل چانک روی دیسک
            size_t expected_size;              // اندازه مورد انتظار چانک برای تأیید
        } download;
        
        struct {                 // ساختار داده برای وظایف محاسبه هش
            uint8_t *data;       // اشاره‌گر به داده‌هایی که باید هش شوند
            size_t data_size;    // اندازه داده‌ها
            uint8_t hash[HASH_LENGTH];  // بافر خروجی برای ذخیره هش محاسبه شده
        } hash;
        
        // struct { ... } write_file;
        // struct { ... } delete_file;
        
    } data;                      // انتهای union - نام متغیر "data"
    
    // Callback برای اطلاع‌رسانی اتمام وظیفه
    void (*callback)(void*, int);  // اشاره‌گر به تابع callback
                                   // وقتی وظیفه کامل شد (موفق یا ناموفق) این تابع فراخوانی می‌شود
                                   // پارامتر اول: callback_arg، پارامتر دوم: کد نتیجه (۰=موفق)
    
    void *callback_arg;            // آرگومان دلخواه که به تابع callback ارسال می‌شود
                                   // می‌تواند هر داده‌ای باشد که caller نیاز دارد
    
    int task_id;                   // شناسه یکتا و افزایشی برای این وظیفه
                                   // برای رهگیری و دیباگ استفاده می‌شود
    
    struct task *next;             // اشاره‌گر به وظیفه بعدی در صف پیوندی
                                   // برای پیاده‌سازی صف (queue) با لیست پیوندی استفاده می‌شود
} task_t;

// Thread Pool بهبود یافته (Enhanced Thread Pool)
// این ساختار یک Thread Pool کامل با قابلیت‌های پیشرفته را تعریف می‌کند
typedef struct {
    pthread_t *workers;            // آرایه‌ای از شناسه‌های نخ (thread IDs)
                                   // اشاره‌گر پویا به آرایه نخ‌های کارگر
                                   // انعطاف‌پذیرتر از آرایه ثابت است
    
    int worker_count;              // تعداد فعلی نخ‌های کارگر فعال
                                   // ممکن است کمتر از max_workers باشد
    
    int max_workers;               // حداکثر تعداد نخ‌های مجاز
                                   // برای جلوگیری از overload سیستم استفاده می‌شود
    
    task_t *task_queue_head;       // اشاره‌گر به اولین وظیفه در صف (برای حذف)
                                   // صف به صورت لیست پیوندی یک‌طرفه پیاده‌سازی شده است
    
    task_t *task_queue_tail;       // اشاره‌گر به آخرین وظیفه در صف (برای افزودن)
                                   // افزودن در انتها با O(1) انجام می‌شود
    
    int queue_size;                // تعداد فعلی وظایف در صف
                                   // برای بررسی وضعیت صف استفاده می‌شود
    
    int max_queue_size;            // حداکثر اندازه مجاز صف
                                   // وقتی صف پر شود، submit مسدود می‌شود
    
    int shutdown;                  // پرچم وضعیت خاموشی (Shutdown Flag)
                                   // ۰ = فعال، ۱ = در حال خاموش شدن
                                   // وقتی ۱ باشد، دیگر وظیفه‌ای پذیرفته نمی‌شود
    
    int active_tasks;              // تعداد وظایفی که در حال حاضر توسط نخ‌ها در حال اجرا هستند
                                   // برای پیگیری بار کاری و shutdown ایمن استفاده می‌شود
    
    // مکانیزم‌های همگام‌سازی (Synchronization Mechanisms)
    pthread_mutex_t queue_lock;    // قفل (Mutex) برای دسترسی thread-safe به صف وظایف
                                   // هر دسترسی به صف باید با این قفل محافظت شود
    
    pthread_cond_t queue_not_empty; // متغیر شرطی (Condition Variable)
                                    // نخ‌های کارگر روی این منتظر می‌مانند تا صف خالی نباشد
                                    // وقتی وظیفه‌ای اضافه می‌شود، این سیگنال داده می‌شود
    
    pthread_cond_t queue_not_full;  // متغیر شرطی (Condition Variable)
                                    // تولیدکنندگان (submitکنندگان) روی این منتظر می‌مانند تا صف پر نباشد
                                    // وقتی وظیفه‌ای حذف می‌شود، این سیگنال داده می‌شود
    
    pthread_cond_t queue_empty;    // متغیر شرطی (Condition Variable)
                                    // برای shutdown کامل استفاده می‌شود
                                    // وقتی shutdown فراخوانی می‌شود، روی این منتظر می‌مانیم
                                    // تا همه وظایف کامل شوند
    
    // آمار و متریک‌ها (Statistics and Metrics)
    unsigned long tasks_completed;  // تعداد کل وظایفی که با موفقیت کامل شده‌اند
                                    // برای مانیتورینگ و تنظیم عملکرد استفاده می‌شود
    
    unsigned long tasks_failed;     // تعداد کل وظایفی که با خطا کامل شده‌اند
                                    // برای تشخیص مشکلات سیستم استفاده می‌شود
    
    // می‌توانید متریک‌های دیگر را اینجا اضافه کنید:
    // unsigned long total_execution_time;
    // unsigned long avg_queue_wait_time;
    // unsigned long peak_queue_size;
    
} thread_pool_t;

// متغیر سراسری Thread Pool (Global Thread Pool Variable)
// این یک نمونه سراسری از Thread Pool است که در کل برنامه استفاده می‌شود
// static یعنی این متغیر فقط در این فایل قابل دسترسی است (محدوده فایل)
thread_pool_t g_thread_pool;


// ==================== Thread Pool ====================

// تابع کارگر - این تابع توسط هر نخ کارگر اجرا می‌شود
static void* worker_thread(void *arg) {
    thread_pool_t *pool = (thread_pool_t*)arg; // دریافت اشاره‌گر به ساختار Thread Pool
    
    while (1) {
        task_t *task = NULL; // اشاره‌گر به وظیفه جاری
        
        // گرفتن وظیفه از صف
        pthread_mutex_lock(&pool->queue_lock); // قفل کردن صف برای دسترسی اتمیک
        
        // منتظر وظیفه - تا زمانی که صف خالی است و Thread Pool خاموش نشده
        while (pool->queue_size == 0 && !pool->shutdown) {
            // این شرط حلقه while است نه if، چون ممکن است نخ به صورت کاذب بیدار شود (spurious wakeup)
            pthread_cond_wait(&pool->queue_not_empty, &pool->queue_lock); // منتظر سیگنال پر شدن صف
        }
        
        // بررسی وضعیت خاموشی - اگر Thread Pool در حال خاموش شدن است و صف خالی است
        if (pool->shutdown && pool->queue_size == 0) {
            pthread_mutex_unlock(&pool->queue_lock); // آزاد کردن قفل قبل از خروج
            break; // خروج از حلقه و پایان نخ کارگر
        }
        
        // حذف وظیفه از ابتدای صف (FIFO)
        task = pool->task_queue_head; // گرفتن اولین وظیفه در صف
        if (task) {
            // به‌روزرسانی اشاره‌گر ابتدای صف
            pool->task_queue_head = task->next;
            if (pool->task_queue_head == NULL) {
                // اگر صف خالی شد، انتهای صف را نیز NULL کنیم
                pool->task_queue_tail = NULL;
            }
            pool->queue_size--; // کاهش اندازه صف
            pool->active_tasks++; // افزایش تعداد وظایف در حال اجرا
            
            // اطلاع به تولیدکنندگان که صف دیگر پر نیست
            pthread_cond_signal(&pool->queue_not_full);
        }
        
        pthread_mutex_unlock(&pool->queue_lock); // آزاد کردن قفل صف
        
        // اجرای وظیفه اگر وظیفه‌ای وجود داشت
        if (task) {
            execute_task(task); // اجرای منطق وظیفه
            
            // کاهش تعداد وظایف فعال و به‌روزرسانی آمار
            pthread_mutex_lock(&pool->queue_lock); // قفل کردن برای دسترسی امن
            pool->active_tasks--; // کاهش تعداد وظایف در حال اجرا
            pool->tasks_completed++; // افزایش شمارنده وظایف تکمیل شده
            
            // اگر Thread Pool در حال خاموش شدن است و همه کارها تمام شده‌اند
            if (pool->shutdown && pool->queue_size == 0 && pool->active_tasks == 0) {
                pthread_cond_signal(&pool->queue_empty); // اطلاع به مدیریت که همه کارها تمام شد
            }
            pthread_mutex_unlock(&pool->queue_lock); // آزاد کردن قفل
            
            // آزادسازی حافظه تخصیص‌یافته برای وظیفه
            free_task(task);
        }
    }
    
    return NULL; // پایان نخ کارگر
}

// اجرای وظیفه بر اساس نوع آن
static void execute_task(task_t *task) {
    switch (task->type) {
        case TASK_TYPE_UPLOAD_CHUNK:
            process_upload_chunk_task(task); // پردازش چانک آپلود
            break;
            
        case TASK_TYPE_DOWNLOAD_CHUNK:
            process_download_chunk_task(task); // پردازش چانک دانلود
            break;
            
        case TASK_TYPE_COMPUTE_HASH:
            compute_hash_task(task); // محاسبه هش
            break;
            
        case TASK_TYPE_WRITE_FILE:
            write_file_task(task); // نوشتن فایل
            break;
            
        // می‌توانید انواع وظایف دیگر را اینجا اضافه کنید
    }
    
    // فراخوانی تابع callback اگر توسط ایجادکننده وظیفه تنظیم شده باشد
    if (task->callback) {
        task->callback(task->callback_arg, 0); // 0 = کد موفقیت
    }
}
//_hash
// پردازش چانک آپلود - این تابع توسط نخ‌های کارگر فراخوانی می‌شود
static void process_upload_chunk_task(task_t *task) {
    // استخراج پارامترها از ساختار وظیفه
    upload_state_t *state = task->data.upload.upload_state; // وضعیت آپلود
    int chunk_index = task->data.upload.chunk_index; // اندیس چانک (۰-بنیاد)
    uint8_t *data = task->data.upload.data; // داده‌های چانک
    size_t data_size = task->data.upload.data_size; // اندازه داده‌ها
    
    // ۱. محاسبه هش چانک با استفاده از الگوریتم هش‌سازی
    uint8_t hash[HASH_LENGTH]; // بافر برای ذخیره هش باینری
    compute_hash(data, data_size, hash); // محاسبه هش
    
    char hash_hex[65]; // بافر برای ذخیره هش به فرمت hex (64 کاراکتر + NULL ترمینیتور)
    hash_to_hex(hash, hash_hex); // تبدیل هش باینری به رشته hex
    
    // ۲. بررسی deduplication (جلوگیری از ذخیره تکراری)
    pthread_mutex_lock(&chunk_table_lock); // قفل کردن جدول چانک‌ها
    
    chunk_entry_t *chunk = find_chunk_by_hash(hash_hex); // جستجوی چانک با هش مشابه
    if (chunk) {
        // چانک از قبل وجود دارد - افزایش شمارنده ارجاع
        pthread_mutex_lock(&chunk->lock); // قفل مخصوص این چانک
        chunk->ref_count++; // افزایش ref_count
        pthread_mutex_unlock(&chunk->lock); // آزاد کردن قفل چانک
        printf("[DEDUP] چانک تکراری یافت شد، ref_count افزایش یافت: %s\n", hash_hex);
    } else {
        // چانک جدید - ایجاد رکورد جدید
        chunk = create_chunk_entry(hash_hex, data, data_size);
        printf("[NEW] چانک جدید ایجاد شد: %s\n", hash_hex);
    }
    
    pthread_mutex_unlock(&chunk_table_lock); // آزاد کردن قفل جدول چانک‌ها
    
    // ۳. به‌روزرسانی وضعیت آپلود
    pthread_mutex_lock(&state->lock); // قفل کردن وضعیت آپلود
    
    // ذخیره اطلاعات چانک در آرایه چانک‌های وضعیت آپلود
    if (chunk_index < state->chunk_count) {
        state->chunks[chunk_index].index = chunk_index; // ذخیره اندیس
        state->chunks[chunk_index].size = data_size; // ذخیره اندازه
        memcpy(state->chunks[chunk_index].hash, hash, HASH_LENGTH); // کپی هش
        state->chunks[chunk_index].file_path = strdup(chunk->file_path); // کپی مسیر فایل
    } else {
        printf("[WARN] اندیس چانک خارج از محدوده: %d >= %d\n", 
               chunk_index, state->chunk_count);
    }
    
    state->chunks_processed++; // افزایش شمارنده چانک‌های پردازش شده
    
    // بررسی اتمام پردازش تمام چانک‌ها
    if (state->chunks_processed == state->chunk_count) {
        pthread_cond_signal(&state->all_chunks_processed); // اطلاع به منتظران
        printf("[UPLOAD] تمام چانک‌ها پردازش شدند\n");
    }
    
    pthread_mutex_unlock(&state->lock); // آزاد کردن قفل وضعیت آپلود
    
    // ۴. ثبت لاگ برای رهگیری
    printf("[THREAD] چانک آپلود %d پردازش شد (هش: %s، تعداد ارجاع: %d)\n",
           chunk_index, hash_hex, chunk->ref_count);
}

// پردازش چانک دانلود - این تابع توسط نخ‌های کارگر فراخوانی می‌شود
static void process_download_chunk_task(task_t *task) {
    // استخراج پارامترها از ساختار وظیفه
    download_state_t *state = task->data.download.download_state; // وضعیت دانلود
    int chunk_index = task->data.download.chunk_index; // اندیس چانک
    const char *chunk_hash = task->data.download.chunk_hash; // هش انتظاری چانک
    const char *chunk_path = task->data.download.chunk_path; // مسیر فایل چانک
    size_t expected_size = task->data.download.expected_size; // اندازه مورد انتظار
    
    // ۱. خواندن چانک از دیسک
    uint8_t *buffer = malloc(expected_size); // تخصیص حافظه برای داده‌های چانک
    if (!buffer) {
        printf("[ERROR] خطا در تخصیص حافظه برای چانک %d\n", chunk_index);
        // فراخوانی callback با کد خطا
        if (task->callback) {
            task->callback(task->callback_arg, -1); // -1 = خطای حافظه
        }
        return; // خروج زودرس
    }
    
    // باز کردن فایل چانک برای خواندن
    int fd = open(chunk_path, O_RDONLY);
    if (fd < 0) {
        printf("[ERROR] نتوانستم فایل چانک را باز کنم: %s\n", chunk_path);
        free(buffer); // آزادسازی حافظه تخصیص‌یافته
        if (task->callback) {
            task->callback(task->callback_arg, -1); // -1 = خطای باز کردن فایل
        }
        return; // خروج زودرس
    }
    
    // خواندن داده‌ها از فایل
    ssize_t bytes_read = read(fd, buffer, expected_size);
    close(fd); // بستن فایل توصیف‌گر
    
    // بررسی صحت اندازه خوانده شده
    if (bytes_read != expected_size) {
        printf("[ERROR] اندازه خوانده شده (%ld) با اندازه مورد انتظار (%zu) مطابقت ندارد\n", 
               bytes_read, expected_size);
        free(buffer); // آزادسازی حافظه
        if (task->callback) {
            task->callback(task->callback_arg, -1); // -1 = خطای خواندن
        }
        return; // خروج زودرس
    }
    
    // ۲. تأیید صحت هش - اطمینان از عدم خرابی داده
    uint8_t computed_hash[HASH_LENGTH]; // بافر برای هش محاسبه شده
    compute_hash(buffer, expected_size, computed_hash); // محاسبه هش داده‌های خوانده شده
    
    char computed_hash_hex[65]; // بافر برای هش محاسبه شده به فرمت hex
    hash_to_hex(computed_hash, computed_hash_hex); // تبدیل به رشته
    
    // مقایسه هش محاسبه شده با هش انتظاری
    if (strcmp(computed_hash_hex, chunk_hash) != 0) {
        printf("[ERROR] هش چانک %d مطابقت ندارد: انتظار %s، دریافت %s\n",
               chunk_index, chunk_hash, computed_hash_hex);
        free(buffer); // آزادسازی حافظه
        if (task->callback) {
            task->callback(task->callback_arg, -2); // -2 = خطای هش نامطابقت
        }
        return; // خروج زودرس
    }
    
    // ۳. ذخیره داده‌های تأیید شده در وضعیت دانلود
    pthread_mutex_lock(&state->lock); // قفل کردن وضعیت دانلود
    
    if (chunk_index < state->chunk_count) {
        // ذخیره داده‌های چانک در ساختار وضعیت
        state->chunks[chunk_index].data = buffer; // اشاره‌گر به داده‌ها
        state->chunks[chunk_index].data_size = expected_size; // اندازه داده‌ها
        state->chunks[chunk_index].verified = 1; // علامت‌گذاری به عنوان تأیید شده
        
        state->chunks_processed++; // افزایش شمارنده چانک‌های پردازش شده
        
        // بررسی اتمام پردازش تمام چانک‌ها
        if (state->chunks_processed == state->chunk_count) {
            pthread_cond_signal(&state->all_chunks_ready); // اطلاع به منتظران
            printf("[DOWNLOAD] تمام چانک‌ها دانلود و تأیید شدند\n");
        }
    } else {
        printf("[WARN] اندیس چانک خارج از محدوده در دانلود: %d >= %d\n",
               chunk_index, state->chunk_count);
        free(buffer); // آزادسازی حافظه چون اندیس نامعتبر است
    }
    
    pthread_mutex_unlock(&state->lock); // آزاد کردن قفل وضعیت دانلود
    
    // ۴. ثبت لاگ موفقیت
    printf("[THREAD] چانک دانلود %d تأیید و بارگذاری شد (%zu بایت)\n",
           chunk_index, expected_size);
    
    // توجه: callback در execute_task فراخوانی می‌شود، نه اینجا
}

/**
 * تابع process_download_chunk_task برای Thread Pool (متناسب با ساختار task_t شما)
 */
/*
 static void process_download_chunk_task(task_t *task){
    // *** استخراج پارامترها
    download_state_t *state = task->data.download.download_state;
    int chunk_index = task->data.download.chunk_index;
    const char *chunk_hash = task->data.download.chunk_hash;
    const char *chunk_path = task->data.download.chunk_path;
    size_t expected_size = task->data.download.expected_size;
    
    printf("[THREAD POOL] Processing chunk %d\n", chunk_index);
    
    // *** ۱. راستی‌آزمایی چانک (با قفل‌گذاری ایمن)
    // تبدیل hash string به باینری
    uint8_t hash_binary[HASH_LENGTH];
    for (int i = 0; i < HASH_LENGTH; i++) {
        sscanf(chunk_hash + i * 2, "%2hhx", &hash_binary[i]);
    }
    
    if (verify_chunk_safe(chunk_path, hash_binary) < 0) {
        printf("[THREAD POOL] ERROR: Chunk %d verification failed\n", chunk_index);
        if (task->callback) {
            task->callback(task->callback_arg, -1);
        }
        return;
    }
    
    // *** ۲. خواندن چانک (با قفل‌گذاری ایمن)
    long chunk_file_size;
    unsigned char *chunk_data = read_chunk_file_safe(chunk_path, &chunk_file_size);
    if (!chunk_data || chunk_file_size != expected_size) {
        printf("[THREAD POOL] ERROR: Cannot read chunk %d file: %s\n", 
               chunk_index, chunk_path);
        if (chunk_data) free(chunk_data);
        if (task->callback) {
            task->callback(task->callback_arg, -1);
        }
        return;
    }
    
    // *** ۳. ذخیره داده‌ها در وضعیت دانلود (تحت قفل)
    pthread_mutex_lock(&state->lock);
    
    if (chunk_index < state->total_chunks) {
        state->chunk_data[chunk_index] = chunk_data;
        state->chunk_sizes[chunk_index] = chunk_file_size;
        // chunk_ready در callback تنظیم می‌شود
    } else {
        free(chunk_data);
    }
    
    pthread_mutex_unlock(&state->lock);
    
    printf("[THREAD POOL] Chunk %d processed successfully (%ld bytes)\n", 
           chunk_index, chunk_file_size);
    
    // *** ۴. فراخوانی callback (در execute_task انجام می‌شود)
    // موفقیت - callback با کد 0 فراخوانی می‌شود
}
*/
// ارسال وظیفه به Thread Pool برای اجرا
int thread_pool_submit_task(task_t *task) {
    // بررسی پارامترهای ورودی
    if (!task || g_thread_pool.shutdown) {
        printf("[ERROR] تلاش برای ارسال وظیفه نامعتبر یا در هنگام خاموشی\n");
        return -1; // خطا
    }
    
    pthread_mutex_lock(&g_thread_pool.queue_lock); // قفل کردن صف
    
    // منتظر شدن تا صف پر نباشد (اگر حداکثر ظرفیت پر باشد)
    while (g_thread_pool.queue_size >= g_thread_pool.max_queue_size && !g_thread_pool.shutdown) {
        pthread_cond_wait(&g_thread_pool.queue_not_full, &g_thread_pool.queue_lock);
    }
    
    // بررسی مجدد خاموشی (ممکن است در حین منتظر ماندن اتفاق افتاده باشد)
    if (g_thread_pool.shutdown) {
        pthread_mutex_unlock(&g_thread_pool.queue_lock);
        printf("[WARN] ارسال وظیفه لغو شد زیرا Thread Pool در حال خاموش شدن است\n");
        return -1; // خطا
    }
    
    // افزودن وظیفه به انتهای صف (FIFO)
    task->next = NULL; // اطمینان از اینکه وظیفه جدید آخرین است
    
    if (g_thread_pool.task_queue_tail) {
        // اگر صف خالی نیست، به انتهای صف اضافه کن
        g_thread_pool.task_queue_tail->next = task;
        g_thread_pool.task_queue_tail = task;
    } else {
        // اگر صف خالی است، هم ابتدا و هم انتها را تنظیم کن
        g_thread_pool.task_queue_head = task;
        g_thread_pool.task_queue_tail = task;
    }
    
    g_thread_pool.queue_size++; // افزایش اندازه صف
    
    // بیدار کردن یک نخ کارگر که منتظر وظیفه است
    pthread_cond_signal(&g_thread_pool.queue_not_empty);
    
    pthread_mutex_unlock(&g_thread_pool.queue_lock); // آزاد کردن قفل
    
    printf("[THREAD POOL] وظیفه %d به صف اضافه شد (اندازه صف: %d)\n",
           task->task_id, g_thread_pool.queue_size);
    
    return task->task_id; // بازگشت شناسه وظیفه برای رهگیری
}

// ایجاد وظیفه آپلود چانک
task_t* create_upload_chunk_task(upload_state_t *state, int chunk_index,
                                 uint8_t *data, size_t data_size,
                                 void (*callback)(void*, int), void *cb_arg) {
    // تخصیص حافظه برای ساختار وظیفه
    task_t *task = calloc(1, sizeof(task_t)); // استفاده از calloc برای صفر کردن فیلدها
    if (!task) {
        printf("[ERROR] خطا در تخصیص حافظه برای وظیفه آپلود\n");
        return NULL;
    }
    
    // پر کردن فیلدهای وظیفه
    task->type = TASK_TYPE_UPLOAD_CHUNK; // تعیین نوع وظیفه
    task->data.upload.upload_state = state; // ذخیره وضعیت آپلود
    task->data.upload.chunk_index = chunk_index; // ذخیره اندیس چانک
    
    // تخصیص و کپی داده‌های چانک
    task->data.upload.data = malloc(data_size);
    if (!task->data.upload.data) {
        printf("[ERROR] خطا در تخصیص حافظه برای داده‌های چانک\n");
        free(task); // آزادسازی حافظه ساختار وظیفه
        return NULL;
    }
    memcpy(task->data.upload.data, data, data_size); // کپی داده‌ها
    task->data.upload.data_size = data_size; // ذخیره اندازه
    
    // ذخیره callback و آرگومان آن (اگر وجود داشته باشد)
    task->callback = callback;
    task->callback_arg = cb_arg;
    
    // تولید شناسه یکتا برای وظیفه
    task->task_id = generate_task_id();
    
    printf("[TASK] وظیفه آپلود چانک %d ایجاد شد (شناسه: %d، اندازه: %zu بایت)\n",
           chunk_index, task->task_id, data_size);
    
    return task;
}

// ایجاد وظیفه دانلود چانک
task_t* create_download_chunk_task(download_state_t *state, int chunk_index,
                                   const char *chunk_hash, const char *chunk_path,
                                   size_t expected_size,
                                   void (*callback)(void*, int), void *cb_arg) {
    // تخصیص حافظه برای ساختار وظیفه
    task_t *task = calloc(1, sizeof(task_t));
    if (!task) {
        printf("[ERROR] خطا در تخصیص حافظه برای وظیفه دانلود\n");
        return NULL;
    }
    
    // پر کردن فیلدهای وظیفه
    task->type = TASK_TYPE_DOWNLOAD_CHUNK; // تعیین نوع وظیفه
    task->data.download.download_state = state; // ذخیره وضعیت دانلود
    task->data.download.chunk_index = chunk_index; // ذخیره اندیس چانک
    
    // کپی هش چانک با بررسی طول
    strncpy(task->data.download.chunk_hash, chunk_hash, 64);
    task->data.download.chunk_hash[64] = '\0'; // اطمینان از پایان‌رشته
    
    // کپی مسیر فایل چانک با بررسی طول
    strncpy(task->data.download.chunk_path, chunk_path, 511);
    task->data.download.chunk_path[511] = '\0'; // اطمینان از پایان‌رشته
    
    task->data.download.expected_size = expected_size; // ذخیره اندازه مورد انتظار
    
    // ذخیره callback و آرگومان آن (اگر وجود داشته باشد)
    task->callback = callback;
    task->callback_arg = cb_arg;
    
    // تولید شناسه یکتا برای وظیفه
    task->task_id = generate_task_id();
    
    printf("[TASK] وظیفه دانلود چانک %d ایجاد شد (شناسه: %d، هش: %.8s...)\n",
           chunk_index, task->task_id, chunk_hash);
    
    return task;
}

// تابع کمکی برای تولید شناسه یکتا و افزایشی برای وظایف
static int generate_task_id(void) {
    static atomic_int next_id = 1; // متغیر اتمی برای جلوگیری از race condition
    return atomic_fetch_add(&next_id, 1); // افزایش اتمی و بازگشت مقدار قبلی
}

// آزادسازی حافظه تخصیص‌یافته برای یک وظیفه
static void free_task(task_t *task) {
    if (!task) return; // بررسی اشاره‌گر NULL
    
    // آزادسازی داده‌های خاص هر نوع وظیفه
    switch (task->type) {
        case TASK_TYPE_UPLOAD_CHUNK:
            free(task->data.upload.data); // آزادسازی داده‌های چانک آپلود
            break;
            
        case TASK_TYPE_DOWNLOAD_CHUNK:
            // داده‌های دانلود در وضعیت دانلود ذخیره می‌شوند، نه اینجا
            // بنابراین اینجا چیزی آزاد نمی‌کنیم
            break;
            
        case TASK_TYPE_COMPUTE_HASH:
            free(task->data.hash.data); // آزادسازی داده‌های هش
            break;
            
        case TASK_TYPE_WRITE_FILE:
            free(task->data.upload.data); // آزادسازی داده‌های نوشتن فایل
            break;
            
        default:
            printf("[WARN] نوع وظیفه ناشناخته در free_task: %d\n", task->type);
            break;
    }
    
    free(task); // آزادسازی خود ساختار وظیفه
    // توجه: task->callback_arg توسط caller مدیریت می‌شود
}
//#############################################################################
//#############################################################################
//############################# commons.c ################################################
//#############################################################################
//#############################################################################
//در این فایل بخشی از کد که بین آپلود و دانلود مشترک استفاده می شود قرار دارد




// ایجاد پوشه
void ensure_dir(const char *path)
{
    struct stat st;
    if (stat(path, &st) == -1)
    {
        mkdir(path, 0755);
    }
}

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


// ذخیره مانیفست (با نوشتن اتمیک)
int store_manifest(const char *cid, const char *manifest_json)
{
    ensure_dir(MANIFESTS_DIR);
    
    // مسیر فایل اصلی و موقت
    char manifest_path[256];
    char temp_path[256];

    snprintf(manifest_path, sizeof(manifest_path), "%s/%s.json", MANIFESTS_DIR, cid);
    snprintf(temp_path, sizeof(temp_path), "%s/%s.tmp", MANIFESTS_DIR, cid);

    // استفاده از الگوی نوشتن اتمیک
    
    // نوشتن در فایل موقت
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
//==========================================================================
// محاسبه هش (از BLAKE3 یا SHA256)
void calculate_hash(const uint8_t *data, size_t len, uint8_t *hash) {
    /*
    
    if (strcmp(HASH_ALGO , "sha256") == 0) // comaring tow strings, return 0 if equal
    {
        SHA256(data, len, hash);
    } 
    else if (strcmp(HASH_ALGO , "blake3") == 0)
    {
        // پیش‌فرض: BLAKE3
        // فاطمه این تکه کدی که با هم زدیمه

        blake3_hasher hasher;
        blake3_hasher_init(&hasher);
        blake3_hasher_update(&hasher, data, len);
        blake3_hasher_finalize(&hasher, hash, HASH_LENGTH);
    }*/
    
    //----because i cant compile with blake3
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
//=====================================================
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
                           "  \"hash_algo\": \"%s\",\n"
                           "  \"chunk_size\": %d,\n"
                           "  \"total_size\": %lld,\n"
                           "  \"filename\": \"%s\",\n"
                           "  \"chunks\": [\n",
                           HASH_ALGO,CHUNK_SIZE, state->file_total_size, state->file_name);

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
    // تولید CID
    return generate_cid(*manifest_json);


}
//=========================================================

// تولید CID
char *generate_cid(const char *manifest_json) {
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

    /*
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
   
    return cid;*/
}

//#############################################################################
//#############################################################################
//################################## upload.c ###########################################
//#############################################################################
//#############################################################################
//در این فایل بخشی از کد که تنها برای آپلود استفاده می شود قرار دارد


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

// ==================== پردازش آپلود ====================
//
upload_state_t * handle_upload_start(int client_fd, const uint8_t *payload, uint32_t len) {
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
    return state;
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
        state->chunk_count = (len + _SIZE - 1) / CHUNK_SIZE;
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
//#############################################################################
//#############################################################################
//############################## download.c ###############################################
//#############################################################################
//#############################################################################
#include "structs.c"

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

/**
 * پاک‌سازی وضعیت دانلود
 */
void cleanup_download_state(download_state_t *state){
    if (!state) return;
    
    // *** آزادسازی CID
    if (state->cid) free((void*)state->cid);
    
    // *** آزادسازی مانیفست
    if (state->manifest) {
        cleanup_manifest_info(state->manifest);
        free(state->manifest);
    }
    
    // *** آزادسازی آرایه چانک‌ها
    if (state->chunk_data) {
        for (int i = 0; i < state->total_chunks; i++) {
            if (state->chunk_data[i]) free(state->chunk_data[i]);
        }
        free(state->chunk_data);
    }
    
    if (state->chunk_sizes) free(state->chunk_sizes);
    if (state->chunk_ready) free(state->chunk_ready);
    
    if (state->chunk_paths) {
        for (int i = 0; i < state->total_chunks; i++) {
            if (state->chunk_paths[i]) free(state->chunk_paths[i]);
        }
        free(state->chunk_paths);
    }
    
    if (state->chunk_order) free(state->chunk_order);
    
    // *** تخریب مکانیزم‌های همگام‌سازی
    pthread_mutex_destroy(&state->lock);
    pthread_cond_destroy(&state->chunk_ready_cond);
    pthread_cond_destroy(&state->chunk_sent_cond);
    
    // *** آزادسازی خود ساختار
    free(state);
}


/*void handle_download_start(int client_fd, const uint8_t *payload, uint32_t len)
{
    printf("[ENGINE] DOWNLOAD_START: cid=\"%.*s\"\n", (int)len, (char *)payload);
    fflush(stdout);

    // استخراج CID از payload
    char *cid = malloc(len + 1);
    if (!cid)
    {
        printf("[ENGINE] ERROR: Failed to allocate CID\n");
        break;
    }
    memcpy(cid, payload, len);
    cid[len] = '\0';

    // خواندن مانیفست
    char *manifest_json = read_manifest_file(cid);
    if (!manifest_json)
    {
        printf("[ENGINE] ERROR: Manifest not found for CID: %s\n", cid);
        free(cid);
        break;
    }

    // تجزیه مانیفست
    struct manifest_info manifest;
    memset(&manifest, 0, sizeof(manifest)); // مقداردهی اولیه ساختار مانیفست

    if (parse_manifest(manifest_json, &manifest) < 0)
    {
        printf("[ENGINE] ERROR: Failed to parse manifest\n");
        free(manifest_json);
        free(cid);
        break;
    }

    printf("[ENGINE] Downloading file: %s, chunks: %d, total size: %lld\n",
            manifest.filename, manifest.chunk_count, manifest.total_size);

    // ارسال چانک‌ها به ترتیب
    for (int i = 0; i < manifest.chunk_count; i++)
    {
        struct file_part *chunk = &manifest.chunks[i];

        // راستی‌آزمایی چانک
        if (verify_chunk(chunk->file_location, chunk->hash_code) < 0)
        {
            printf("[ENGINE] ERROR: Chunk %d verification failed\n", chunk->part_num);
            continue; // از این چانک عبور می‌کنیم
        }

        // خواندن چانک از فایل
        FILE *chunk_file = fopen(chunk->file_location, "rb");
        if (!chunk_file)
        {
            printf("[ENGINE] ERROR: Cannot open chunk file: %s\n", chunk->file_location);
            continue;
        }

        fseek(chunk_file, 0, SEEK_END);
        long chunk_file_size = ftell(chunk_file);
        fseek(chunk_file, 0, SEEK_SET);

        unsigned char *chunk_data = malloc(chunk_file_size);
        if (!chunk_data)
        {
            fclose(chunk_file);
            continue;
        }

        fread(chunk_data, 1, chunk_file_size, chunk_file);
        fclose(chunk_file);

        // ایجاد payload برای DOWNLOAD_CHUNK
        // فرمت: [index:4byte][size:4byte][data:variable]
        int header_size = 8; // 4 + 4
        unsigned char *chunk_payload = malloc(header_size + chunk_file_size);
        if (!chunk_payload)
        {
            free(chunk_data);
            continue;
        }

        // پر کردن هدر
        uint32_t network_index = htonl(chunk->part_num); // to big_endian
        uint32_t network_size = htonl(chunk_file_size);  // to big_endian

        memcpy(chunk_payload, &network_index, 4);               // شماره چانک
        memcpy(chunk_payload + 4, &network_size, 4);            // اندازه داده
        memcpy(chunk_payload + 8, chunk_data, chunk_file_size); // داده چانک

        // ارسال چانک
        if (send_frame(cfd, OP_DOWNLOAD_CHUNK, chunk_payload, header_size + chunk_file_size) < 0) // chunk_payload: داده //header_size + chunk_file_size: طول کل
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
        usleep(1000); // 1ms
    }

    // ارسال پایان دانلود
    printf("[ENGINE] Download completed, sending DONE\n");
    send_frame(cfd, OP_DOWNLOAD_DONE, NULL, 0); // به کلاینت می‌گوید تمام چانک‌ها ارسال شده‌اند

    // پاکسازی
    cleanup_manifest_info(&manifest);
    free(manifest_json);
    free(cid);
}*/


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

    size_t bytes_read = fread(manifest_json, 1, file_size, file);
    if (bytes_read != file_size) {
        free(manifest_json);
        fclose(file);
        printf("[ENGINE] ERROR: Failed to read manifest");
        return;
    }
    manifest_json[file_size] = '\0';

    fclose(file);
    return manifest_json;
}


//  تابع برای خواندن مانیفست با قفل‌گذاری
char *read_manifest_file_safe(const char *cid)
{
    manifest_read_lock(cid); //  قفل مانیفست

    char *manifest_json = read_manifest_file(cid);

    manifest_unlock(cid); //  آزاد کردن قفل
    return manifest_json;
}
//=============================================================================
// ==================== مدیریت manifest_table ====================


/**
 * پیدا کردن یا ایجاد entry مانیفست در جدول
 * این تابع برای خواندن مانیفست در دانلود استفاده می‌شود
 */
manifest_entry_t* find_or_create_manifest_entry(const char *cid) {
    if (!cid) {
        printf("[ERROR] NULL CID provided to find_or_create_manifest_entry\n");
        return NULL;
    }
    
    manifest_entry_t *entry = NULL;
    int found_index = -1;
    
    // *** ۱. قفل کردن جدول مانیفست‌ها
    pthread_mutex_lock(&manifest_table_lock);
    
    // *** ۲. جستجو در جدول
    for (int i = 0; i < manifest_table_size; i++) {
        if (manifest_table[i] && strcmp(manifest_table[i]->cid, cid) == 0) {
            entry = manifest_table[i];
            found_index = i;
            break;
        }
    }
    
    // *** ۳. اگر پیدا شد، افزایش ref_count
    if (entry) {
        pthread_mutex_lock(&entry->lock);
        entry->ref_count++;
        entry->last_access = time(NULL);
        printf("[MANIFEST] Found manifest for CID %s, ref_count: %d -> %d\n", 
               cid, entry->ref_count - 1, entry->ref_count);
        pthread_mutex_unlock(&entry->lock);
        pthread_mutex_unlock(&manifest_table_lock);
        return entry;
    }
    
    // *** ۴. اگر پیدا نشد، entry جدید ایجاد کن
    printf("[MANIFEST] Creating new manifest entry for CID: %s\n", cid);
    
    entry = malloc(sizeof(manifest_entry_t));
    if (!entry) {
        printf("[ERROR] Failed to allocate manifest entry for CID: %s\n", cid);
        pthread_mutex_unlock(&manifest_table_lock);
        return NULL;
    }
    
    // مقداردهی اولیه
    strncpy(entry->cid, cid, sizeof(entry->cid) - 1);
    entry->cid[sizeof(entry->cid) - 1] = '\0';
    entry->manifest_info = NULL;  // هنوز بارگذاری نشده
    entry->ref_count = 1;         // اولین ارجاع
    entry->last_access = time(NULL);
    entry->marked_for_deletion = 0;
    pthread_mutex_init(&entry->lock, NULL);
    
    // *** ۵. افزودن به جدول
    manifest_table_size++;
    manifest_entry_t **new_table = realloc(manifest_table, 
                                          manifest_table_size * sizeof(manifest_entry_t*));
    
    if (!new_table && manifest_table_size > 0) {
        printf("[ERROR] Failed to expand manifest table for CID: %s\n", cid);
        free(entry);
        pthread_mutex_unlock(&manifest_table_lock);
        return NULL;
    }
    
    manifest_table = new_table;
    manifest_table[manifest_table_size - 1] = entry;
    
    printf("[MANIFEST] Created new manifest entry for %s (table size: %d)\n", 
           cid, manifest_table_size);
    
    pthread_mutex_unlock(&manifest_table_lock);
    return entry;
}

/**
 * بارگذاری مانیفست از فایل و ذخیره در entry
 */
int load_manifest_into_entry(manifest_entry_t *entry) {
    if (!entry || !entry->cid[0]) {
        printf("[ERROR] Invalid manifest entry\n");
        return -1;
    }
    
    // *** اگر از قبل بارگذاری شده، نیازی نیست دوباره بارگذاری کنیم
    pthread_mutex_lock(&entry->lock);
    if (entry->manifest_info) {
        printf("[MANIFEST] Manifest already loaded for CID: %s\n", entry->cid);
        pthread_mutex_unlock(&entry->lock);
        return 0;
    }
    pthread_mutex_unlock(&entry->lock);
    
    // *** ۱. خواندن فایل مانیفست
    char manifest_path[256];
    snprintf(manifest_path, sizeof(manifest_path), "manifests/%s.json", entry->cid);
    
    FILE *f = fopen(manifest_path, "r");
    if (!f) {
        printf("[ERROR] Manifest file not found: %s\n", manifest_path);
        return -1;
    }
    
    // خواندن کامل فایل
    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    char *manifest_json = malloc(file_size + 1);
    if (!manifest_json) {
        fclose(f);
        printf("[ERROR] Failed to allocate memory for manifest JSON\n");
        return -1;
    }
    
    fread(manifest_json, 1, file_size, f);
    manifest_json[file_size] = '\0';
    fclose(f);
    
    // *** ۲. تجزیه مانیفست
    manifest_info_t *manifest = malloc(sizeof(manifest_info_t));
    if (!manifest) {
        free(manifest_json);
        printf("[ERROR] Failed to allocate manifest info\n");
        return -1;
    }
    
    memset(manifest, 0, sizeof(manifest_info_t));
    
    if (parse_manifest(manifest_json, manifest) < 0) {
        free(manifest_json);
        free(manifest);
        printf("[ERROR] Failed to parse manifest for CID: %s\n", entry->cid);
        return -1;
    }
    
    free(manifest_json);
    
    // *** ۳. ذخیره در entry (تحت قفل)
    pthread_mutex_lock(&entry->lock);
    entry->manifest_info = manifest;
    pthread_mutex_unlock(&entry->lock);
    
    printf("[MANIFEST] Successfully loaded manifest for CID: %s\n", entry->cid);
    return 0;
}

/**
 * کاهش ref_count برای یک مانیفست
 */
int decrement_manifest_ref(const char *cid) {
    if (!cid) {
        printf("[ERROR] NULL CID in decrement_manifest_ref\n");
        return -1;
    }
    
    manifest_entry_t *entry = NULL;
    int found_index = -1;
    
    // *** ۱. قفل جدول
    pthread_mutex_lock(&manifest_table_lock);
    
    // *** ۲. جستجو
    for (int i = 0; i < manifest_table_size; i++) {
        if (manifest_table[i] && strcmp(manifest_table[i]->cid, cid) == 0) {
            entry = manifest_table[i];
            found_index = i;
            break;
        }
    }
    
    if (!entry) {
        printf("[WARNING] Manifest entry not found for CID: %s\n", cid);
        pthread_mutex_unlock(&manifest_table_lock);
        return -1;
    }
    
    // *** ۳. قفل entry و کاهش ref_count
    pthread_mutex_lock(&entry->lock);
    
    if (entry->ref_count > 0) {
        entry->ref_count--;
        entry->last_access = time(NULL);
        
        printf("[MANIFEST] Decremented ref_count for %s: %d -> %d\n", 
               cid, entry->ref_count + 1, entry->ref_count);
        
        // *** ۴. اگر ref_count به صفر رسید
        if (entry->ref_count == 0) {
            printf("[MANIFEST] Manifest %s has zero refs, marking for deletion\n", cid);
            entry->marked_for_deletion = 1;
            
            // *** ۵. حذف از جدول
            pthread_mutex_unlock(&entry->lock);
            
            if (found_index >= 0 && found_index < manifest_table_size) {
                // جابجا کردن عناصر
                for (int i = found_index; i < manifest_table_size - 1; i++) {
                    manifest_table[i] = manifest_table[i + 1];
                }
                manifest_table_size--;
                
                // کاهش اندازه آرایه
                manifest_entry_t **new_table = realloc(manifest_table, 
                                                      manifest_table_size * sizeof(manifest_entry_t*));
                if (new_table || manifest_table_size == 0) {
                    manifest_table = new_table;
                }
            }
            
            pthread_mutex_unlock(&manifest_table_lock);
            
            // *** ۶. پاک‌سازی entry (می‌تواند تأخیری باشد)
            schedule_manifest_cleanup(entry);
            return 0;
        }
        
        pthread_mutex_unlock(&entry->lock);
        pthread_mutex_unlock(&manifest_table_lock);
        return 0;
        
    } else {
        printf("[WARNING] Manifest %s already has ref_count = 0\n", cid);
        pthread_mutex_unlock(&entry->lock);
        pthread_mutex_unlock(&manifest_table_lock);
        return -1;
    }
}

// *** تابع task برای پاک‌سازی
void cleanup_manifest_task(void *arg) {
    manifest_entry_t *entry = (manifest_entry_t*)arg;
    
    pthread_mutex_lock(&entry->lock);
    
    // *** آزادسازی اطلاعات مانیفست
    if (entry->manifest_info) {
        cleanup_manifest_info(entry->manifest_info);
        free(entry->manifest_info);
        entry->manifest_info = NULL;
    }
    
    printf("[MANIFEST] Cleaned up manifest for CID: %s\n", entry->cid);
    
    pthread_mutex_unlock(&entry->lock);
    
    // *** تخریب قفل و آزادسازی حافظه
    pthread_mutex_destroy(&entry->lock);
    free(entry);
}
/**
 * برنامه‌ریزی پاک‌سازی مانیفست
 */
void schedule_manifest_cleanup(manifest_entry_t *entry) {
    if (!entry) return;
    
    printf("[MANIFEST] Scheduling cleanup for manifest: %s\n", entry->cid);
    
    
    
    // *** ارسال به Thread Pool یا اجرای مستقیم
    // گزینه ۱: Thread Pool
    // task_t *task = create_cleanup_task(entry, cleanup_manifest_task);
    // thread_pool_submit_task(task);
    
    // گزینه ۲: نخ جدید
    pthread_t thread;
    if (pthread_create(&thread, NULL, (void*(*)(void*))cleanup_manifest_task, entry) == 0) {
        pthread_detach(thread);
    } else {
        cleanup_manifest_task(entry);
    }
}

/**
 * پیدا کردن مانیفست (برای دانلود)
 * این تابع جایگزین read_manifest_file در download_start می‌شود
 */
manifest_info_t* get_manifest_for_download(const char *cid) {
    if (!cid) return NULL;
    
    // *** ۱. پیدا کردن یا ایجاد entry
    manifest_entry_t *entry = find_or_create_manifest_entry(cid);
    if (!entry) {
        printf("[ERROR] Failed to get manifest entry for CID: %s\n", cid);
        return NULL;
    }
    
    // *** ۲. بارگذاری مانیفست اگر نیاز باشد
    if (load_manifest_into_entry(entry) < 0) {
        // اگر بارگذاری شکست خورد، ref_count را کاهش بده
        decrement_manifest_ref(cid);
        return NULL;
    }
    
    // *** ۳. بازگرداندن اشاره‌گر به اطلاعات مانیفست
    // نکته: caller نباید این داده‌ها را آزاد کند!
    return entry->manifest_info;
}

/**
 * ثبت مانیفست جدید (برای آپلود)
 */
int register_new_manifest(const char *cid, manifest_info_t *manifest_info) {
    if (!cid || !manifest_info) {
        printf("[ERROR] Invalid parameters for register_new_manifest\n");
        return -1;
    }
    
    // *** ۱. پیدا کردن یا ایجاد entry
    manifest_entry_t *entry = find_or_create_manifest_entry(cid);
    if (!entry) {
        printf("[ERROR] Failed to create manifest entry for new manifest: %s\n", cid);
        return -1;
    }
    
    // *** ۲. ذخیره اطلاعات مانیفست (تحت قفل)
    pthread_mutex_lock(&entry->lock);
    
    // اگر از قبل مانیفستی وجود دارد، آزادش کن
    if (entry->manifest_info) {
        cleanup_manifest_info(entry->manifest_info);
        free(entry->manifest_info);
    }
    
    entry->manifest_info = manifest_info;
    printf("[MANIFEST] Registered new manifest for CID: %s\n", cid);
    
    pthread_mutex_unlock(&entry->lock);
    
    return 0;
}

/**
 * پاک‌سازی دوره‌ای جدول مانیفست‌ها
 */
void garbage_collect_manifest_table() {
    time_t now = time(NULL);
    time_t deletion_delay = 600; // 10 دقیقه تأخیر
    
    printf("[GC] Starting garbage collection for manifest table\n");
    
    pthread_mutex_lock(&manifest_table_lock);
    
    int deleted_count = 0;
    int new_size = 0;
    manifest_entry_t **new_table = malloc(manifest_table_size * sizeof(manifest_entry_t*));
    
    for (int i = 0; i < manifest_table_size; i++) {
        manifest_entry_t *entry = manifest_table[i];
        if (!entry) continue;
        
        pthread_mutex_lock(&entry->lock);
        
        if (entry->ref_count == 0 && 
            entry->marked_for_deletion && 
            (now - entry->last_access) > deletion_delay) {
            
            // *** پاک‌سازی این entry
            printf("[GC] Collecting manifest: %s\n", entry->cid);
            
            if (entry->manifest_info) {
                cleanup_manifest_info(entry->manifest_info);
                free(entry->manifest_info);
            }
            
            pthread_mutex_unlock(&entry->lock);
            pthread_mutex_destroy(&entry->lock);
            free(entry);
            
            deleted_count++;
            
        } else {
            // *** نگه داشتن entry
            new_table[new_size++] = entry;
            pthread_mutex_unlock(&entry->lock);
        }
    }
    
    // *** جایگزینی جدول
    free(manifest_table);
    manifest_table = new_table;
    manifest_table_size = new_size;
    
    pthread_mutex_unlock(&manifest_table_lock);
    
    printf("[GC] Deleted %d manifest entries, %d remain\n", 
           deleted_count, manifest_table_size);
}

/**
 * چاپ وضعیت جدول مانیفست‌ها
 */
void print_manifest_table_status() {
    pthread_mutex_lock(&manifest_table_lock);
    
    printf("=== Manifest Table Status (%d entries) ===\n", manifest_table_size);
    for (int i = 0; i < manifest_table_size; i++) {
        manifest_entry_t *entry = manifest_table[i];
        if (entry) {
            pthread_mutex_lock(&entry->lock);
            printf("  [%d] %s: ref_count=%d, has_manifest=%d, marked=%d\n",
                   i, entry->cid, entry->ref_count, 
                   (entry->manifest_info != NULL), entry->marked_for_deletion);
            pthread_mutex_unlock(&entry->lock);
        }
    }
    printf("========================================\n");
    
    pthread_mutex_unlock(&manifest_table_lock);
}

// ==================== اصلاح download_start برای استفاده از manifest_table ====================

/**
 * نسخه اصلاح شده download_start با استفاده از manifest_table
 */
download_state_t* download_start(int client_fd, const char *cid) {
    printf("[ENGINE] DOWNLOAD_START: cid=\"%s\"\n", cid);
    
    // *** ۱. گرفتن قفل خواندن برای مانیفست
    int lock_result = manifest_read_lock(cid);
    if (lock_result != LOCK_SUCCESS) {
        printf("[ENGINE] ERROR: Failed to acquire manifest read lock\n");
        send_error(client_fd, "خطا در دسترسی به مانیفست");
        return NULL;
    }
    
    // *** ۲. ایجاد و مقداردهی اولیه وضعیت دانلود
    download_state_t *state = malloc(sizeof(download_state_t));
    if (!state) {
        printf("[ENGINE] ERROR: Failed to allocate download state\n");
        manifest_unlock(cid, 0);// آزاد کردن قفل خواندن
        return NULL;
    }
    
    memset(state, 0, sizeof(download_state_t));
    state->client_fd = client_fd;
    state->cid = strdup(cid);
    
    // *** ۳. گرفتن مانیفست از manifest_table
    state->manifest = get_manifest_for_download(cid);
    if (!state->manifest) {
        printf("[ENGINE] ERROR: Failed to get manifest for CID: %s\n", cid);
        free(state->cid);
        free(state);
        manifest_unlock(cid, 0);
        return NULL;
    }
    
    // *** ۴. آزاد کردن قفل مانیفست
    manifest_unlock(cid, 0);
    
    state->total_chunks = state->manifest->chunk_count;
    
    printf("[ENGINE] Downloading file: %s, chunks: %d, total size: %lld\n",
           state->manifest->filename, state->manifest->chunk_count, 
           state->manifest->total_size);
    
    // *** بقیه کد مانند قبل...
    // (تخصیص آرایه‌ها، مقداردهی همگام‌سازی، افزایش refcount بلاک‌ها)
    // *** تجزیه مانیفست
    state->manifest = malloc(sizeof(manifest_info_t));
    if (!state->manifest) {
        printf("[ENGINE] ERROR: Failed to allocate manifest info\n");
        free(manifest_json);
        free(state->cid);
        free(state);
        manifest_unlock(cid, 0);
        return NULL;
    }
    
    memset(state->manifest, 0, sizeof(manifest_info_t));
    
    if (parse_manifest(manifest_json, state->manifest) < 0) {
        printf("[ENGINE] ERROR: Failed to parse manifest\n");
        free(state->manifest);
        free(manifest_json);
        free(state->cid);
        free(state);
        manifest_unlock(cid, 0);
        return NULL;
    }
    
    free(manifest_json); // دیگر نیازی به JSON خام نیست
    
    state->total_chunks = state->manifest->chunk_count;
    
    printf("[ENGINE] Downloading file: %s, chunks: %d, total size: %lld\n",
           state->manifest->filename, state->manifest->chunk_count, 
           state->manifest->total_size);
    
    // *** تخصیص آرایه‌ها برای مدیریت چانک‌ها
    state->chunk_data = calloc(state->total_chunks, sizeof(uint8_t*));
    state->chunk_sizes = calloc(state->total_chunks, sizeof(size_t));
    state->chunk_ready = calloc(state->total_chunks, sizeof(int));
    state->chunk_paths = calloc(state->total_chunks, sizeof(char*));
    state->chunk_order = malloc(state->total_chunks * sizeof(int));
    
    if (!state->chunk_data || !state->chunk_sizes || !state->chunk_ready || 
        !state->chunk_paths || !state->chunk_order) {
        printf("[ENGINE] ERROR: Failed to allocate chunk arrays\n");
        cleanup_download_state(state);
        manifest_unlock(cid, 0);
        return NULL;
    }
    
    // *** پر کردن آرایه مسیرها و اندیس‌ها
    for (int i = 0; i < state->total_chunks; i++) {
        state->chunk_paths[i] = strdup(state->manifest->chunks[i].file_location);
        state->chunk_order[i] = i;
    }
    
    // *** مقداردهی اولیه مکانیزم‌های همگام‌سازی
    pthread_mutex_init(&state->lock, NULL);
    pthread_cond_init(&state->chunk_ready_cond, NULL);
    pthread_cond_init(&state->chunk_sent_cond, NULL);
    
    state->chunks_ready_count = 0;
    state->chunks_sent_count = 0;
    state->next_expected_index = 0;
    
    // *** افزایش reference count برای تمام بلاک‌ها (تحت قفل جدول بلاک‌ها)
    pthread_mutex_lock(&chunk_table_lock);
    for (int i = 0; i < state->total_chunks; i++) {
        char hash_hex[65];
        for (int j = 0; j < HASH_LENGTH; j++) {
            sprintf(hash_hex + j * 2, "%02x", state->manifest->chunks[i].hash_code[j]);
        }
        increment_ref_count(hash_hex);
    }
    pthread_mutex_unlock(&chunk_table_lock);
    
    // *** آزاد کردن قفل مانیفست (خواندن کامل شد)
    manifest_unlock(cid, 0);
    
    printf("[ENGINE] Download started successfully for CID: %s\n", cid);
    return state;
    
}
/**
 * فاز ۲: دانلود چانک‌ها با استفاده از Thread Pool
 * این تابع برای هر چانک یک task ایجاد کرده و به Thread Pool ارسال می‌کند
 */
 // *** تعریف callback برای اطلاع اتمام
void chunk_download_callback(void *arg, int result) {
    int *chunk_index_ptr = (int*)arg;
    int chunk_index = *chunk_index_ptr;
    
    pthread_mutex_lock(&state->lock);
    
    if (result == 0) {
        // *** چانک با موفقیت پردازش شد
        state->chunk_ready[chunk_index] = 1; // علامت‌گذاری به عنوان آماده
        state->chunks_ready_count++;
        
        printf("[CALLBACK] Chunk %d ready (%d/%d)\n", 
            chunk_index, state->chunks_ready_count, state->total_chunks);
        
        // *** اگر این چانک، چانک بعدی مورد انتظار است، سیگنال بده
        if (chunk_index == state->next_expected_index) {
            pthread_cond_signal(&state->chunk_ready_cond);
        }
        
        // *** اگر همه چانک‌ها آماده شدند، سیگنال بده
        if (state->chunks_ready_count == state->total_chunks) {
            pthread_cond_broadcast(&state->chunk_ready_cond);
        }
    } else {
        // *** خطا در پردازش چانک
        printf("[CALLBACK] ERROR: Failed to process chunk %d (error: %d)\n", 
            chunk_index, result);
        state->chunk_ready[chunk_index] = -1; // علامت خطا
        // در اینجا می‌توانید تصمیم بگیرید که کل دانلود لغو شود یا ادامه یابد
    }
    
    pthread_mutex_unlock(&state->lock);
    free(chunk_index_ptr);
}
        
int download_chunks(download_state_t *state)
{
    if (!state) {
        return -1;
    }
    
    printf("[ENGINE] Starting parallel download of %d chunks\n", state->total_chunks);
    
    int tasks_submitted = 0;
    
    // *** برای هر چانک یک task ایجاد و به Thread Pool ارسال کن
    for (int i = 0; i < state->total_chunks; i++) {
        struct file_part *chunk = &state->manifest->chunks[i];
        
        // *** تبدیل hash به string hex
        char hash_hex[65];
        for (int j = 0; j < HASH_LENGTH; j++) {
            sprintf(hash_hex + j * 2, "%02x", chunk->hash_code[j]);
        }
        
        
        // *** ذخیره اندیس برای callback
        int *chunk_index_ptr = malloc(sizeof(int));
        *chunk_index_ptr = i;
        
        // *** ایجاد task برای Thread Pool
        task_t *task = create_download_chunk_task(
            state,                    // وضعیت دانلود
            i,                        // اندیس چانک
            hash_hex,                 // هش چانک
            state->chunk_paths[i],    // مسیر فایل
            chunk->size,              // اندازه چانک
            chunk_download_callback,  // callback
            chunk_index_ptr           // آرگومان callback (اندیس)
        );
        
        if (!task) {
            printf("[ENGINE] ERROR: Failed to create task for chunk %d\n", i);
            free(chunk_index_ptr);
            continue;
        }
        
        // *** ارسال task به Thread Pool
        if (thread_pool_submit_task(task) == 0) {
            tasks_submitted++;
            printf("[ENGINE] Submitted chunk %d task to thread pool (ID: %d)\n", 
                   i, task->task_id);
        } else {
            printf("[ENGINE] ERROR: Failed to submit task for chunk %d\n", i);
            free_task(task);
            free(chunk_index_ptr);
        }
    }
    
    printf("[ENGINE] %d/%d chunk tasks submitted to thread pool\n", 
           tasks_submitted, state->total_chunks);
    
    return tasks_submitted;
}
// ==================== اصلاح download_done برای استفاده از manifest_table ====================

/**
 * نسخه اصلاح شده download_done با استفاده از manifest_table
 */
void download_done(download_state_t *state) {
    if (!state) return;
    
    printf("[ENGINE] Finalizing download for CID: %s\n", state->cid);
    
    // *** ۱. ارسال پیام اتمام
        // *** ارسال پیام اتمام دانلود به کلاینت

    send_frame(state->client_fd, OP_DOWNLOAD_DONE, NULL, 0);
    
    // *** ۲. کاهش refcount بلاک‌ها
    pthread_mutex_lock(&chunk_table_lock);
    for (int i = 0; i < state->total_chunks; i++) {
        char hash_hex[65];
        hash_to_hex(state->manifest->chunks[i].hash_code, hash_hex);
        decrement_ref_count(hash_hex);
    }
    pthread_mutex_unlock(&chunk_table_lock);
    
    // *** ۳. کاهش refcount مانیفست
    decrement_manifest_ref(state->cid);
    
    
    
    // آزادسازی آرایه‌ها...
    // (اما state->manifest را آزاد نکن!)    
    pthread_mutex_destroy(&state->lock);
    pthread_cond_destroy(&state->chunk_ready_cond);
    pthread_cond_destroy(&state->chunk_sent_cond);
    
    
    
    printf("[ENGINE] Download finalized for CID: %s\n", state->cid);
    // *** ۴. پاک‌سازی وضعیت دانلود (اما نه manifest_info!)
    // چون manifest_info متعلق به manifest_table است
    free(state->cid);
    free(state);

}



//=========================================================================
/**
 * کاهش شمارنده ارجاع برای یک بلاک
 * وقتی ref_count به صفر رسید، بلاک حذف می‌شود
 * 
 * @param hash_hex هش بلاک (64 کاراکتر hex)
 * @return 0 در موفقیت، -1 در خطا
 */
int decrement_ref_count(const char *hash_hex) {
    if (!hash_hex || strlen(hash_hex) != 64) {
        printf("[ERROR] Invalid hash for decrement_ref_count: %s\n", 
               hash_hex ? hash_hex : "NULL");
        return -1;
    }
    
    chunk_entry_t *chunk = NULL;
    int found_index = -1;
    
    // *** ۱. قفل کردن جدول چانک‌ها
    pthread_mutex_lock(&chunk_table_lock);
    
    // *** ۲. جستجوی بلاک در جدول
    for (int i = 0; i < chunk_table_size; i++) {
        if (chunk_table[i] && strcmp(chunk_table[i]->hash_hex, hash_hex) == 0) {
            chunk = chunk_table[i];
            found_index = i;
            break;
        }
    }
    
    if (!chunk) {
        printf("[WARNING] Block not found in chunk_table: %s\n", hash_hex);
        pthread_mutex_unlock(&chunk_table_lock);
        return -1;
    }
    
    // *** ۳. قفل مخصوص این بلاک برای تغییر ref_count
    pthread_mutex_lock(&chunk->lock);
    
    // *** ۴. کاهش ref_count (اما نه کمتر از صفر)
    if (chunk->ref_count > 0) {
        chunk->ref_count--;
        printf("[REFCOUNT] Decremented ref_count for %s: %d -> %d\n", 
               hash_hex, chunk->ref_count + 1, chunk->ref_count);
        
        // *** ۵. اگر ref_count به صفر رسید، بلاک را حذف کن
        if (chunk->ref_count == 0) {
            printf("[REFCOUNT] Block %s has zero refs, scheduling deletion\n", hash_hex);
            
            // *** ۵.۱ علامت‌گذاری برای حذف
            chunk->marked_for_deletion = 1;
            chunk->last_access = time(NULL);
            
            // *** ۵.۲ حذف از جدول (ایمن)
            // روش ۱: NULL کردن اشاره‌گر (ساده)
            // chunk_table[found_index] = NULL;
            
            // روش ۲: فشرده‌سازی جدول (بهتر)
            if (found_index >= 0 && found_index < chunk_table_size) {
                // جابجا کردن عناصر بعدی
                for (int i = found_index; i < chunk_table_size - 1; i++) {
                    chunk_table[i] = chunk_table[i + 1];
                }
                chunk_table_size--;
                
                // کاهش اندازه آرایه
                chunk_entry_t **new_table = realloc(chunk_table, 
                                                   chunk_table_size * sizeof(chunk_entry_t*));
                if (new_table || chunk_table_size == 0) {
                    chunk_table = new_table;
                }
                // اگر realloc شکست خورد، chunk_table هنوز معتبر است اما یک slot اضافی دارد
            }
            
            // *** ۵.۳ آزاد کردن قفل جدول قبل از عملیات I/O
            pthread_mutex_unlock(&chunk_table_lock);
            
            // *** ۵.۴ آزاد کردن قفل بلاک
            pthread_mutex_unlock(&chunk->lock);
            
            // *** ۵.۵ برنامه‌ریزی حذف فایل (در یک task جداگانه)
            schedule_chunk_deletion(chunk);
            
            return 0;
        }
        
        // *** ۶. اگر ref_count > 0، فقط قفل‌ها را آزاد کن
        pthread_mutex_unlock(&chunk->lock);
        pthread_mutex_unlock(&chunk_table_lock);
        
        return 0;
        
    } else {
        printf("[WARNING] Block %s already has ref_count = 0\n", hash_hex);
        pthread_mutex_unlock(&chunk->lock);
        pthread_mutex_unlock(&chunk_table_lock);
        return -1;
    }
}

/**
 * افزایش شمارنده ارجاع برای یک بلاک
 */
int increment_ref_count(const char *hash_hex) {
    if (!hash_hex || strlen(hash_hex) != 64) {
        printf("[ERROR] Invalid hash for increment_ref_count: %s\n", 
               hash_hex ? hash_hex : "NULL");
        return -1;
    }
    
    // *** قفل کردن جدول چانک‌ها
    pthread_mutex_lock(&chunk_table_lock);
    
    chunk_entry_t *chunk = NULL;
    
    // *** جستجوی بلاک در جدول
    for (int i = 0; i < chunk_table_size; i++) {
        if (chunk_table[i] && strcmp(chunk_table[i]->hash_hex, hash_hex) == 0) {
            chunk = chunk_table[i];
            break;
        }
    }
    
    if (!chunk) {
        // *** بلاک جدید - باید ایجاد شود (معمولاً در آپلود)
        printf("[REFCOUNT] Creating new block entry for: %s\n", hash_hex);
        
        chunk = malloc(sizeof(chunk_entry_t));
        if (!chunk) {
            pthread_mutex_unlock(&chunk_table_lock);
            return -1;
        }
        
        // *** مقداردهی اولیه بلاک جدید
        strncpy(chunk->hash_hex, hash_hex, 64);
        chunk->hash_hex[64] = '\0';
        
        // *** ساخت مسیر فایل بر اساس هش (مطابق با پروژه)
        char file_path[512];
        snprintf(file_path, sizeof(file_path), 
                 "blocks/%.2s/%.2s/%s",
                 hash_hex, hash_hex + 2, hash_hex);
        
        chunk->file_path = strdup(file_path);
        chunk->ref_count = 1;  // اولین ارجاع
        chunk->marked_for_deletion = 0;
        chunk->last_access = time(NULL);
        pthread_mutex_init(&chunk->lock, NULL);
        
        // *** افزودن به جدول
        chunk_table_size++;
        chunk_entry_t **new_table = realloc(chunk_table, 
                                           chunk_table_size * sizeof(chunk_entry_t*));
        if (!new_table && chunk_table_size > 0) {
            free(chunk->file_path);
            free(chunk);
            pthread_mutex_unlock(&chunk_table_lock);
            return -1;
        }
        
        chunk_table = new_table;
        chunk_table[chunk_table_size - 1] = chunk;
        
        printf("[REFCOUNT] Created new block entry for %s (ref_count=1)\n", hash_hex);
        
    } else {
        // *** بلاک موجود - افزایش ref_count
        pthread_mutex_lock(&chunk->lock);
        chunk->ref_count++;
        chunk->last_access = time(NULL);
        pthread_mutex_unlock(&chunk->lock);
        
        printf("[REFCOUNT] Incremented ref_count for %s: %d -> %d\n", 
               hash_hex, chunk->ref_count - 1, chunk->ref_count);
    }
    
    pthread_mutex_unlock(&chunk_table_lock);
    return 0;
}

/**
 * برنامه‌ریزی حذف چانک در یک task جداگانه
 */
void schedule_chunk_deletion(chunk_entry_t *chunk) {
    if (!chunk) return;
    
    printf("[DELETION] Scheduling deletion of block: %s\n", chunk->hash_hex);
    
    // *** ایجاد آرگومان برای task حذف
    typedef struct {
        char file_path[512];
        char hash_hex[65];
        char *original_path;  // برای free کردن
    } deletion_arg_t;
    
    deletion_arg_t *arg = malloc(sizeof(deletion_arg_t));
    if (!arg) {
        // اگر حافظه کم است، حذف مستقیم انجام بده
        delete_chunk_immediately(chunk);
        return;
    }
    
    strncpy(arg->file_path, chunk->file_path, sizeof(arg->file_path) - 1);
    arg->file_path[511] = '\0';
    strncpy(arg->hash_hex, chunk->hash_hex, sizeof(arg->hash_hex) - 1);
    arg->hash_hex[64] = '\0';
    arg->original_path = chunk->file_path;  // ذخیره برای free کردن بعداً
    
    // *** تابع task برای حذف
    void delete_chunk_task(void *task_arg) {
        deletion_arg_t *arg = (deletion_arg_t*)task_arg;
        
        printf("[THREAD] Deleting block file: %s\n", arg->file_path);
        
        // *** ۱. حذف فایل از دیسک
        if (unlink(arg->file_path) == 0) {
            printf("[DELETION] Block file deleted: %s\n", arg->file_path);
            
            // *** ۲. پاک‌سازی دایرکتوری‌های خالی
            cleanup_empty_directories(arg->file_path);
        } else {
            printf("[ERROR] Failed to delete block file: %s (errno: %d)\n", 
                   arg->file_path, errno);
            // می‌توانید تصمیم بگیرید که دوباره تلاش کنید
        }
        
        // *** ۳. آزادسازی حافظه
        free(arg->original_path);
        free(arg);
    }
    
    // *** دو گزینه برای اجرا:
    
    // گزینه ۱: استفاده از Thread Pool شما
    task_t *deletion_task = create_deletion_task(arg, delete_chunk_task);
    if (deletion_task) {
        thread_pool_submit_task(deletion_task);
        printf("[DELETION] Deletion task submitted to thread pool\n");
    } else {
        // اگر ایجاد task شکست خورد، مستقیم حذف کن
        delete_chunk_task(arg);
    }
    
    // گزینه ۲: ایجاد نخ جدید
    /*
    pthread_t thread;
    if (pthread_create(&thread, NULL, (void*(*)(void*))delete_chunk_task, arg) == 0) {
        pthread_detach(thread);
    } else {
        delete_chunk_task(arg);
    }
    */
    
    // *** ۴. آزادسازی ساختار chunk (بعد از حذف فایل)
    pthread_mutex_destroy(&chunk->lock);
    free(chunk);
}

/**
 * حذف فوری چانک (بدون task جداگانه)
 */
void delete_chunk_immediately(chunk_entry_t *chunk) {
    if (!chunk) return;
    
    printf("[IMMEDIATE] Deleting block immediately: %s\n", chunk->hash_hex);
    
    // حذف فایل
    if (unlink(chunk->file_path) == 0) {
        printf("[IMMEDIATE] Block file deleted: %s\n", chunk->file_path);
        cleanup_empty_directories(chunk->file_path);
    }
    
    // پاک‌سازی حافظه
    free(chunk->file_path);
    pthread_mutex_destroy(&chunk->lock);
    free(chunk);
}

/**
 * پاک‌سازی دوره‌ای جدول چانک‌ها
 */
void garbage_collect_chunk_table() {
    time_t now = time(NULL);
    time_t deletion_delay = 300; // 5 دقیقه تأخیر
    
    printf("[GC] Starting garbage collection for chunk table\n");
    
    pthread_mutex_lock(&chunk_table_lock);
    
    int deleted_count = 0;
    int new_size = 0;
    
    // *** ایجاد جدول جدید بدون بلاک‌های حذف شده
    chunk_entry_t **new_table = malloc(chunk_table_size * sizeof(chunk_entry_t*));
    
    for (int i = 0; i < chunk_table_size; i++) {
        chunk_entry_t *chunk = chunk_table[i];
        if (!chunk) continue;
        
        pthread_mutex_lock(&chunk->lock);
        
        if (chunk->ref_count == 0 && 
            chunk->marked_for_deletion && 
            (now - chunk->last_access) > deletion_delay) {
            
            // *** بلاک برای حذف آماده است
            printf("[GC] Collecting block: %s\n", chunk->hash_hex);
            
            // حذف فایل
            if (unlink(chunk->file_path) == 0) {
                cleanup_empty_directories(chunk->file_path);
            }
            
            // آزادسازی حافظه
            free(chunk->file_path);
            pthread_mutex_unlock(&chunk->lock);
            pthread_mutex_destroy(&chunk->lock);
            free(chunk);
            
            deleted_count++;
            
        } else {
            // *** بلاک هنوز مورد نیاز است
            new_table[new_size++] = chunk;
            pthread_mutex_unlock(&chunk->lock);
        }
    }
    //handle_download
    // *** جایگزینی جدول قدیمی با جدید
    free(chunk_table);
    chunk_table = new_table;
    chunk_table_size = new_size;
    
    pthread_mutex_unlock(&chunk_table_lock);
    
    printf("[GC] Deleted %d blocks, %d blocks remain\n", 
           deleted_count, chunk_table_size);
}

/**
 * پیدا کردن یک چانک در جدول
 */
chunk_entry_t* find_chunk_in_table(const char *hash_hex) {
    if (!hash_hex) return NULL;
    
    pthread_mutex_lock(&chunk_table_lock);
    
    chunk_entry_t *chunk = NULL;
    for (int i = 0; i < chunk_table_size; i++) {
        if (chunk_table[i] && strcmp(chunk_table[i]->hash_hex, hash_hex) == 0) {
            chunk = chunk_table[i];
            break;
        }
    }
    
    pthread_mutex_unlock(&chunk_table_lock);
    return chunk;
}

/**
 * چاپ وضعیت جدول چانک‌ها (برای دیباگ)
 */
void print_chunk_table_status() {
    pthread_mutex_lock(&chunk_table_lock);
    
    printf("=== Chunk Table Status (%d entries) ===\n", chunk_table_size);
    for (int i = 0; i < chunk_table_size; i++) {
        chunk_entry_t *chunk = chunk_table[i];
        if (chunk) {
            pthread_mutex_lock(&chunk->lock);
            printf("  [%d] %s: ref_count=%d, marked=%d, last_access=%ld\n",
                   i, chunk->hash_hex, chunk->ref_count, 
                   chunk->marked_for_deletion, chunk->last_access);
            pthread_mutex_unlock(&chunk->lock);
        }
    }
    printf("=====================================\n");
    
    pthread_mutex_unlock(&chunk_table_lock);
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
    
    //cleanup:
    // پاکسازی وظیفه
    free(task->chunk_hash);
    free(task->chunk_path);
    free(task);
}
/*//  تابع برای پردازش دانلود همزمان
void handle_download_start(int client_fd, const char *cid) {

    printf("[ENGINE] Starting download for CID: %s\n", cid);
    fflush(stdout);

    // ۱. خواندن مانیفست از فایل
    // خواندن مانیفست با قفل‌گذاری
    char *manifest_json = read_manifest_file_safe(cid);
    if (!manifest_json)
    {
        printf("[ENGINE] ERROR: Manifest not found for CID: %s\n", cid);
        return;
    }

    
    // ۲. پارس کردن JSON
    int chunk_count = 0;
    char **chunk_hashes = NULL;
    int *chunk_sizes = NULL;
    
    if (parse_manifest_json_simple(manifest_json, &chunk_count, &chunk_hashes, &chunk_sizes) != 0) {
        free(manifest_json);
        send_error(client_fd, "E_INTERNAL", "Failed to parse manifest");
        printf("[ENGINE] ERROR: Failed to parse manifest\n");
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
        //serial sending
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
}*/



/*// پارس کردن ساده JSON مانیفست
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
}*/
//#############################################################################
//#############################################################################
//#############################################################################
//#############################################################################
//#############################################################################

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
            if (read_n(client_fd, payload, length) <= 0) { perror("read_n"); free(payload); break; }
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
            
            if (download_state) {
                download_chunks(download_state);
                download_done(download_state);
            }


            free(cid);
            //break;
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
