//در این فایل بخشی از کد که بین آپلود و دانلود مشترک استفاده می شود قرار دارد

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
// 3. ساختار وظیفه خواندن چانک برای Thread Pool
typedef struct read_chunk_task {
    download_state_t *dl_state;     // اشاره‌گر به وضعیت دانلود مربوطه
    int chunk_index;                // شماره چانک (0-based)
    char *chunk_hash;               // هش چانک برای تأیید صحت
    char *chunk_path;               // مسیر کامل فایل چانک
    size_t expected_size;           // اندازه مورد انتظار چانک
    struct read_chunk_task *next;   // برای صف پیوندی
} read_chunk_task_t;

// 4. ساختار برای صف وظایف چانک‌ها
typedef struct {
    read_chunk_task_t *head;        // ابتدای صف
    read_chunk_task_t *tail;        // انتهای صف
    int count;                      // تعداد وظایف در صف
    pthread_mutex_t lock;           // قفل صف
    pthread_cond_t not_empty;       // شرط پر نبودن صف
    pthread_cond_t not_full;        // شرط پر بودن صف
    int max_size;                   // حداکثر اندازه صف
} chunk_task_queue_t;

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
