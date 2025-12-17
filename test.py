import socket
import struct
import os

SOCKET_PATH = "/tmp/cengine.sock"

# Protocol opcodes
OP_UPLOAD_START = 0x01
OP_UPLOAD_CHUNK = 0x02
OP_UPLOAD_FINISH = 0x03
OP_UPLOAD_DONE = 0x81

def send_frame(sock, opcode, payload=b''):
    length = len(payload)
    header = struct.pack('>BI', opcode, length)
    sock.sendall(header)
    if payload:
        sock.sendall(payload)

def test_upload():
    # Connect to the engine
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(SOCKET_PATH)
    
    # Send UPLOAD_START
    filename = "test.txt"
    file_data = b"Hello, this is a test file for IPFS project!"
    total_size = len(file_data)
    chunk_size = 256 * 1024
    
    start_payload = f"{filename}|{total_size}|{chunk_size}|sha256".encode()
    print(f"Sending UPLOAD_START: {start_payload}")
    send_frame(sock, OP_UPLOAD_START, start_payload)
    
    # Send UPLOAD_CHUNK (only one chunk for small file)
    index = 0
    index_bytes = struct.pack('>I', index)
    chunk_payload = index_bytes + file_data
    print(f"Sending UPLOAD_CHUNK, size: {len(file_data)}")
    send_frame(sock, OP_UPLOAD_CHUNK, chunk_payload)
    
    # Send UPLOAD_FINISH
    print("Sending UPLOAD_FINISH")
    send_frame(sock, OP_UPLOAD_FINISH)
    
    # Receive response
    while True:
        # Read header
        header = sock.recv(5)
        if len(header) < 5:
            break
            
        opcode, length = struct.unpack('>BI', header)
        print(f"Received opcode: 0x{opcode:02x}, length: {length}")
        
        # Read payload
        payload = b''
        if length > 0:
            payload = sock.recv(length)
        
        if opcode == OP_UPLOAD_DONE:
            print(f"Upload successful! CID: {payload.decode()}")
            break
        elif opcode == 0xFF:  # ERROR
            print(f"Error: {payload.decode()}")
            break
    
    sock.close()

if __name__ == "__main__":
    test_upload()