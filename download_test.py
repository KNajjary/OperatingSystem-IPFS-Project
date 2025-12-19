
# test_upload_download.py
import socket
import struct
import time
import os

def upload_test_file():
    """First upload a small test file"""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect("/tmp/cengine.sock")
    
    filename = "test.txt"
    data = b"Hello, this is a test file for download testing!"
    total_size = len(data)
    
    # UPLOAD_START
    start_msg = struct.pack(">BI", 0x01, len(filename)) + filename.encode()
    sock.sendall(start_msg)
    
    # UPLOAD_CHUNK (just one chunk)
    chunk_msg = struct.pack(">BI", 0x02, 4 + len(data)) + struct.pack(">I", 0) + data
    sock.sendall(chunk_msg)
    
    # UPLOAD_FINISH
    finish_msg = struct.pack(">BI", 0x03, 0)
    sock.sendall(finish_msg)
    
    # Read response
    def recv_exact(n):
        buf = bytearray()
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("engine closed")
            buf.extend(chunk)
        return bytes(buf)
    
    cid = None
    while True:
        header = recv_exact(5)
        op, length = struct.unpack(">BI", header)
        payload = recv_exact(length) if length else b""
        
        if op == 0x81:  # UPLOAD_DONE
            cid = payload.decode('utf-8')
            break
    
    sock.close()
    return cid

def download_file(cid):
    """Download the file we just uploaded"""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect("/tmp/cengine.sock")
    
    # DOWNLOAD_START
    start_msg = struct.pack(">BI", 0x11, len(cid)) + cid.encode()
    sock.sendall(start_msg)
    
    # Read response
    sock.settimeout(2.0)
    
    data_received = b""
    try:
        while True:
            header = sock.recv(5)
            if not header:
                break
            if len(header) < 5:
                print(f"Short header: {header}")
                break
                
            op, length = struct.unpack(">BI", header)
            print(f"Got op=0x{op:02x}, length={length}")
            
            if length > 0:
                payload = sock.recv(length)
                if op == 0x91:  # DOWNLOAD_CHUNK
                    data_received += payload
                    
            if op == 0x92:  # DOWNLOAD_DONE
                break
                
    except socket.timeout:
        print("Timeout")
    
    sock.close()
    return data_received

if __name__ == "__main__":
    # Kill any running engine
    import subprocess
    subprocess.run(["pkill", "-f", "c_engine"], stderr=subprocess.DEVNULL)
    time.sleep(1)
    
    # Start engine
    print("Starting engine...")
    import subprocess
    engine = subprocess.Popen(["./c_engine", "/tmp/cengine.sock"], 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE)
    time.sleep(2)
    
    try:
        # Upload test file
        print("Uploading test file...")
        cid = upload_test_file()
        print(f"Uploaded, got CID: {cid}")
        
        time.sleep(1)
        
        # Download it back
        print(f"\nDownloading CID: {cid}")
        data = download_file(cid)
        print(f"Downloaded {len(data)} bytes: {data}")
        
    finally:
        engine.terminate()
        engine.wait()
        