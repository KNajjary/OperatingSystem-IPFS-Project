import socket
import struct
import time

SOCKET_PATH = "/tmp/cengine.sock"

# مطابق با کد C جدید
OP_DOWNLOAD_START = 0x11
OP_DOWNLOAD_CHUNK = 0x91
OP_DOWNLOAD_DONE = 0x92

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect(SOCKET_PATH)

# ارسال DOWNLOAD_START
cid = b'bqiureifzi6mj6kpije3lv2lw2bm5z3qzxpc2ct6uku64b47fmjdj2w543m'
length = len(cid)
header = struct.pack('>BI', OP_DOWNLOAD_START, length)
sock.sendall(header)
sock.sendall(cid)

print(f"Sent DOWNLOAD_START (0x11) with CID: {cid.decode()}")

# دریافت پاسخ
while True:
    header = sock.recv(5)
    if len(header) < 5:
        break
    
    opcode, length = struct.unpack('>BI', header)
    print(f"Received opcode: 0x{opcode:02x}, length: {length}")
    
    if length > 0:
        payload = sock.recv(length)
        print(f"Payload: {payload[:50]}...")
    
    if opcode == OP_DOWNLOAD_DONE:
        print("Download completed!")
        break

sock.close()