#!/bin/bash
echo "=== Starting OS Project ==="

# پاکسازی سوکت قدیمی
rm -f /tmp/cengine.sock

# اجرای موتور C در پس‌زمینه
echo "1. Starting C engine..."
./c_engine /tmp/cengine.sock &
ENGINE_PID=$!
sleep 3

# اجرای Gateway پایتون
echo "2. Starting Python gateway..."
python3 main.py &
GATEWAY_PID=$!
sleep 3

# تست آپلود
echo "3. Testing upload..."
curl -X POST http://localhost:9000/upload \
  -H "X-Filename: test.txt" \
  --data-binary "Hello from GitHub Codespaces!" \
  -w "\n\n"

echo "✅ Project is running!"
echo "   C Engine PID: $ENGINE_PID"
echo "   Gateway PID: $GATEWAY_PID"
echo "   Access at: http://localhost:9000"
echo -e "\nPress Ctrl+C to stop"

# نگه داشتن اسکریپت
trap "kill $ENGINE_PID $GATEWAY_PID 2>/dev/null; echo 'Stopped'; exit" INT
wait
