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