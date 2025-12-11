#!/bin/bash
echo "=== Final Test Script ==="

# پاکسازی
rm -f /tmp/cengine.sock

# اجرای موتور C
echo "Starting C engine..."
./c_engine /tmp/cengine.sock &
ENGINE_PID=$!
sleep 3

# اجرای Gateway
echo "Starting Python gateway..."
python3 main.py &
GATEWAY_PID=$!
sleep 3

# تست
echo "Testing upload..."
curl -X POST http://localhost:9000/upload \
  -H "X-Filename: test.txt" \
  --data-binary "Test content" \
  -w "\n"

echo -e "\n✅ If you see a CID above, project is working!"
echo "   Engine PID: $ENGINE_PID"
echo "   Gateway PID: $GATEWAY_PID"
echo "   Press Ctrl+C to stop"

trap "kill $ENGINE_PID $GATEWAY_PID 2>/dev/null; echo 'Stopped'; exit" INT
wait
