#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// تعریف ثابت‌ها
// قفل سراسری برای عملیات خاص
//static pthread_mutex_t manifest_table_lock = PTHREAD_MUTEX_INITIALIZER;


// تابع کمکی برای debug
void manifest_lock_debug_print(void) {
    printf("=== Manifest Lock Table Debug ===\n");
    
    pthread_mutex_lock(&g_lock_table.table_lock);
    
    for (int i = 0; i < LOCK_TABLE_SIZE; i++) {
        manifest_lock_entry_t *entry = g_lock_table.locks[i];
        int count = 0;
        
        while (entry != NULL) {
            count++;
            printf("Bucket[%d]: CID='%s', ref_count=%d\n", 
                   i, entry->cid, entry->ref_count);
            entry = entry->next;
        }
        
        if (count > 0) {
            printf("  Total entries in bucket %d: %d\n", i, count);
        }
    }
    
    pthread_mutex_unlock(&g_lock_table.table_lock);
    printf("================================\n");
}




// مقداردهی اولیه
manifest_lock_init();

// آپلود فایل (نوشتن مانیفست)
manifest_write_lock(cid);
// ... نوشتن مانیفست
manifest_unlock(cid, 1); // is_writer = 1

// دانلود فایل (خواندن مانیفست)
manifest_read_lock(cid);
// ... خواندن مانیفست
manifest_unlock(cid, 0); // is_writer = 0

// پاک‌سازی
manifest_lock_destroy();

int lock_result = manifest_write_trylock(cid, OPERATION_TIMEOUT);
    
    // 2. بررسی نتیجه
    if (lock_result != LOCK_SUCCESS) {
        handle_lock_failure(cid, lock_result);
        return -1;
    }
    