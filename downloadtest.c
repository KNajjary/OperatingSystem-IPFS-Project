// 2. ساختار وضعیت دانلود برای هر اتصال کلاینت
typedef struct download_state {
    char *cid;                      // CID درخواستی
    int client_fd;                  // فایل‌دسکریپتور سوکت کلاینت
    manifest_info_t *manifest;      // اشاره‌گر به مانیفست مربوطه
    
    // اطلاعات چانک‌ها
    int total_chunks;               // تعداد کل چانک‌ها
    int *chunk_order;               // آرایه ترتیب ارسال چانک‌ها
    uint8_t **chunk_data;           // آرایه داده‌های چانک‌ها (اشاره‌گر)
    size_t *CHUNCK_BITSs;            // آرایه اندازه هر چانک
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
} download_state_t;
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

// تابع پاکسازی وضعیت دانلود
void free_download_state(download_state_t *state) {
    if (!state) return;
    
    free(state->cid);
    
    // آزادسازی آرایه‌های چانک‌ها
    if (state->chunk_data) {
        for (int i = 0; i < state->total_chunks; i++) {
            if (state->chunk_data[i]) {
                free(state->chunk_data[i]);
            }
        }
        free(state->chunk_data);
    }
    
    if (state->chunk_sizes) free(state->chunk_sizes);
    if (state->chunk_ready) free(state->chunk_ready);
    
    // تخریب mutex و condition
    pthread_mutex_destroy(&state->lock);
    pthread_cond_destroy(&state->chunk_ready_cond);
    
    free(state);
}
// ==================== توابع دانلود سه‌بخشی با استفاده از Thread Pool ====================

/**
 * فاز ۱: شروع دانلود
 * این تابع مانیفست را می‌خواند، قفل‌ها را مدیریت می‌کند و وضعیت دانلود را آماده می‌کند

 
 // wrong lock
download_state_t* download_start(int client_fd, const char *cid)
{
    printf("[ENGINE] DOWNLOAD_START: cid=\"%s\"\n", cid);
    
    // *** گرفتن قفل خواندن برای مانیفست
    int lock_result = manifest_read_lock(cid);
    if (lock_result != LOCK_SUCCESS) {
        printf("[ENGINE] ERROR: Failed to acquire manifest read lock for CID: %s\n", cid);
        send_error(client_fd, "خطا در دسترسی به مانیفست");
        return NULL;
    }
    
    // ایجاد و مقداردهی اولیه وضعیت دانلود
    download_state_t *state = malloc(sizeof(download_state_t));
    if (!state) {
        printf("[ENGINE] ERROR: Failed to allocate download state\n");
        manifest_unlock(cid, 0); // آزاد کردن قفل خواندن
        return NULL;
    }
    
    memset(state, 0, sizeof(download_state_t));
    state->client_fd = client_fd;
    state->cid = strdup(cid);
    
    // *** خواندن مانیفست از فایل (با قفل گرفته شده)
    char *manifest_json = read_manifest_file(cid);
    if (!manifest_json) {
        printf("[ENGINE] ERROR: Manifest not found for CID: %s\n", cid);
        free(state->cid);
        free(state);
        manifest_unlock(cid, 0);
        return NULL;
    }
    
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
    state->CHUNCK_BITSs = calloc(state->total_chunks, sizeof(size_t));
    state->chunk_ready = calloc(state->total_chunks, sizeof(int));
    state->chunk_paths = calloc(state->total_chunks, sizeof(char*));
    state->chunk_order = malloc(state->total_chunks * sizeof(int));
    
    if (!state->chunk_data || !state->CHUNCK_BITSs || !state->chunk_ready || 
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
}*/

/**
 * فاز ۲: دانلود چانک‌ها با استفاده از Thread Pool
 * این تابع برای هر چانک یک task ایجاد کرده و به Thread Pool ارسال می‌کند
 */
/*int download_chunks(download_state_t *state)
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
}*/

/**
 * فاز ۳: اتمام دانلود و پاک‌سازی
 * این تابع منتظر می‌ماند تا همه چانک‌ها ارسال شوند، سپس منابع را آزاد می‌کند
 //wrong lock
void download_done(download_state_t *state)
{
    if (!state) return;
    
    printf("[ENGINE] Finalizing download for CID: %s\n", state->cid);
    
    // *** ارسال پیام اتمام دانلود به کلاینت
    send_frame(state->client_fd, OP_DOWNLOAD_DONE, NULL, 0);
    
    // *** کاهش reference count برای تمام بلاک‌ها (تحت قفل جدول بلاک‌ها)
    pthread_mutex_lock(&chunk_table_lock);
    for (int i = 0; i < state->total_chunks; i++) {
        char hash_hex[65];
        for (int j = 0; j < HASH_LENGTH; j++) {
            sprintf(hash_hex + j * 2, "%02x", state->manifest->chunks[i].hash_code[j]);
        }
        decrement_ref_count(hash_hex);
    }
    pthread_mutex_unlock(&chunk_table_lock);
    
    // *** پاک‌سازی کامل وضعیت دانلود
    cleanup_download_state(state);
    
    printf("[ENGINE] Download finalized for CID: %s\n", state->cid);
}*/

// ==================== تابع اصلی هندلر دانلود (با استفاده از ۳ تابع بالا) ====================

/**
 * تابع اصلی هندلر دانلود که از ۳ تابع مجزا استفاده می‌کند
 */
void handle_download(int cfd, const char *cid)
{
    printf("[ENGINE] ===== Starting 3-phase download for CID: %s ===\n", cid);
    fflush(stdout);

    // فاز ۱: شروع دانلود
    printf("[ENGINE] Phase 1: download_start\n");
    download_state_t *state = download_start(cfd, cid);
    if (!state) {
        printf("[ENGINE] ERROR: download_start failed\n");
        return;
    }
    
    // فاز ۲: دانلود چانک‌ها به صورت موازی
    printf("[ENGINE] Phase 2: download_chunks\n");
    int tasks_submitted = download_chunks(state);
    if (tasks_submitted <= 0) {
        printf("[ENGINE] ERROR: No chunk tasks submitted\n");
        download_done(state);
        return;
    }
    
    // *** حلقه ارسال چانک‌ها به ترتیب
    printf("[ENGINE] Starting to send chunks in order...\n");
    
    while (state->chunks_sent_count < state->total_chunks) {
        pthread_mutex_lock(&state->lock);
        
        // *** منتظر بمان تا چانک بعدی مورد انتظار آماده شود
        while (state->next_expected_index < state->total_chunks && 
               state->chunk_ready[state->next_expected_index] != 1) {
            printf("[ENGINE] Waiting for chunk %d to be ready...\n", 
                   state->next_expected_index);
            pthread_cond_wait(&state->chunk_ready_cond, &state->lock);
        }
        
        // *** بررسی خطا در چانک
        if (state->chunk_ready[state->next_expected_index] == -1) {
            printf("[ENGINE] ERROR: Chunk %d failed, aborting download\n", 
                   state->next_expected_index);
            pthread_mutex_unlock(&state->lock);
            break;
        }
        
        int current_index = state->next_expected_index;
        
        // *** داده‌های چانک را از آرایه بردار
        uint8_t *chunk_data = state->chunk_data[current_index];
        size_t chunk_size = state->CHUNCK_BITSs[current_index];
        
        // *** چانک هنوز در حافظه است (توسط Thread Pool ذخیره شده)
        if (!chunk_data) {
            printf("[ENGINE] ERROR: Chunk %d data is NULL\n", current_index);
            pthread_mutex_unlock(&state->lock);
            continue;
        }
        
        pthread_mutex_unlock(&state->lock);
        
        // *** ایجاد و ارسال فریم چانک
        int header_size = 8;
        unsigned char *chunk_payload = malloc(header_size + chunk_size);
        if (!chunk_payload) {
            printf("[ENGINE] ERROR: Failed to allocate payload for chunk %d\n", 
                   current_index);
            continue;
        }
        
        uint32_t network_index = htonl(current_index);
        uint32_t network_size = htonl(chunk_size);
        
        memcpy(chunk_payload, &network_index, 4);
        memcpy(chunk_payload + 4, &network_size, 4);
        memcpy(chunk_payload + 8, chunk_data, chunk_size);
        
        // *** ارسال چانک به کلاینت
        if (send_frame(cfd, OP_DOWNLOAD_CHUNK, chunk_payload, header_size + chunk_size) < 0) {
            printf("[ENGINE] ERROR: Failed to send chunk %d\n", current_index);
        } else {
            printf("[ENGINE] Sent chunk %d, size: %zu\n", current_index, chunk_size);
        }
        
        free(chunk_payload);
        
        // *** به‌روزرسانی وضعیت ارسال
        pthread_mutex_lock(&state->lock);
        state->chunks_sent_count++;
        state->next_expected_index++;
        
        // *** آزادسازی حافظه چانک (دیگر نیازی نیست)
        free(state->chunk_data[current_index]);
        state->chunk_data[current_index] = NULL;
        
        printf("[ENGINE] Progress: %d/%d chunks sent\n", 
               state->chunks_sent_count, state->total_chunks);
        
        pthread_mutex_unlock(&state->lock);
        
        // *** تاخیر کوچک برای جلوگیری از overload (اختیاری)
        usleep(1000);
    }
    
    // فاز ۳: اتمام دانلود
    printf("[ENGINE] Phase 3: download_done\n");
    download_done(state);
    
    printf("[ENGINE] === Download completed for CID: %s ===\n", cid);
}

// ==================== توابع کمکی ====================

/**
 * پاک‌سازی وضعیت دانلود
 */
void cleanup_download_state(download_state_t *state)
{
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
    
    if (state->CHUNCK_BITSs) free(state->CHUNCK_BITSs);
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

/**
 * تابع process_download_chunk_task برای Thread Pool (متناسب با ساختار task_t شما)
 */
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
        state->CHUNCK_BITSs[chunk_index] = chunk_file_size;
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