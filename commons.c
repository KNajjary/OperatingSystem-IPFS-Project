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
    }
    /*
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
        */
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
