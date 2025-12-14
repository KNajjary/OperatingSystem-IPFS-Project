#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define CID_MAX_LEN 128
#define LOCK_TABLE_SIZE 64  // اندازه جدول هش


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
    int *CHUNCK_BITSs;

    char *filename;
    long long total_size;
    //struct file_part *chunks;               // آرایه اندازه‌های چانک‌ها
} manifest_info_t;

// 5. ساختار برای ذخیره کَش مانیفست‌ها
typedef struct {
    manifest_info_t **table;        // جدول هش برای مانیفست‌ها
    int size;                       // اندازه جدول
    int capacity;                   // ظرفیت جدول
    pthread_mutex_t lock;           // قفل جدول
    int max_capacity;               // حداکثر تعداد مانیفست‌ها در کَش
    manifest_info_t *lru_head;      // برای الگوریتم LRU
    manifest_info_t *lru_tail;      // برای الگوریتم LRU
} manifest_cache_t;

// 6. ساختار برای ادغام‌کننده ترتیبی (Sequential Merger)
typedef struct {
    download_state_t *dl_state;     // وضعیت دانلود مربوطه
    int window_size;                // اندازه پنجره برای ارسال
    int *window;                    // پنجره چانک‌های در حال ارسال
    int window_start;               // شروع پنجره
    int window_end;                 // پایان پنجره
    pthread_mutex_t lock;           // قفل ادغام‌کننده
    pthread_cond_t window_cond;     // شرط تغییر پنجره
} sequential_merger_t;

// ساختار entry برای قفل
typedef struct manifest_lock_entry {
    char cid[CID_MAX_LEN];
    pthread_rwlock_t rwlock;          // قفل خواندن/نوشتن برای این CID
    int ref_count;                    // شمارنده استفاده‌کنندگان
    struct manifest_lock_entry *next; // برای پیوند لیست
} manifest_lock_entry_t;

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
#define LOCK_SUCCESS       0
#define LOCK_FAILED       -1  // خطای عمومی
#define LOCK_TIMEOUT      -2  // timeout رسیده
#define LOCK_NO_MEMORY    -3  // حافظه کافی نیست
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