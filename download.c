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
    
    // *** ۴. پاک‌سازی وضعیت دانلود (اما نه manifest_info!)
    // چون manifest_info متعلق به manifest_table است
    free(state->cid);
    
    // آزادسازی آرایه‌ها...
    // (اما state->manifest را آزاد نکن!)
    
    pthread_mutex_destroy(&state->lock);
    pthread_cond_destroy(&state->chunk_ready_cond);
    pthread_cond_destroy(&state->chunk_sent_cond);
    
    free(state);
    
    printf("[ENGINE] Download finalized for CID: %s\n", state->cid);
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
    handle_download
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