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


