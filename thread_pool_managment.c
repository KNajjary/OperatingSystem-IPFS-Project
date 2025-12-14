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