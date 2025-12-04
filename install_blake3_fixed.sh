#!/bin/bash
echo "=== Installing Blake3 Correctly ==="

# 1. رفتن به پوشه پروژه
cd /workspaces/OperatingSystem-IPFS-Project

# 2. پاکسازی اگر قبلاً نصب شده
echo "Cleaning previous install..."
sudo rm -f /usr/local/include/blake3.h
sudo rm -f /usr/local/lib/libblake3*

# 3. کلون کردن (اگر هنوز نکرده‌اید)
if [ ! -d "BLAKE3" ]; then
    echo "Cloning Blake3 repository..."
    git clone https://github.com/BLAKE3-team/BLAKE3.git
fi

# 4. رفتن به پوشه c و کامپایل
echo "Compiling Blake3..."
cd BLAKE3/c
make

# 5. بررسی ساخت موفق
if [ ! -f "libblake3.a" ]; then
    echo "Error: Failed to compile Blake3!"
    exit 1
fi

# 6. نصب
echo "Installing Blake3 system-wide..."
sudo make install

# 7. به‌روزرسانی کش کتابخانه‌ها
sudo ldconfig

# 8. بازگشت به پوشه پروژه
cd /workspaces/OperatingSystem-IPFS-Project

# 9. تست کتابخانه
echo "Testing Blake3 installation..."
cat > test_blake3_simple.c << 'TESTEOF'
#include <stdio.h>
#include <blake3.h>
int main() {
    printf("Blake3 header found!\n");
    printf("Version: %s\n", blake3_version());
    return 0;
}
TESTEOF

gcc -o test_blake3_simple test_blake3_simple.c -lblake3 2>/dev/null
if [ $? -eq 0 ]; then
    ./test_blake3_simple
    echo "✅ Blake3 installed successfully!"
else
    echo "⚠️  Compilation failed, trying alternative method..."
fi

# 10. پاکسازی تست
rm -f test_blake3_simple.c test_blake3_simple
