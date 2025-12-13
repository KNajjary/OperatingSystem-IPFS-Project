void handle_download_start(int client_fd, const uint8_t *payload, uint32_t len)
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
}