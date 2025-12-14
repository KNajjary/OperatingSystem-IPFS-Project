#python gui_client.py
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import queue
import json
import socket
import struct
from datetime import datetime
import os

class IPFSClientGUI:
    """رابط گرافیکی برای کاربر نهایی"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("IPFS Client - مدیریت فایل‌ها")
        self.root.geometry("900x700")
        
        # اتصال به سرور پایتون
        self.server_host = "localhost"
        self.server_port = 8080
        
        # صف برای ارتباط بین تردها
        self.message_queue = queue.Queue()
        
        # داده‌های وضعیت
        self.active_uploads = {}
        self.active_downloads = {}
        self.file_history = []
        
        # تنظیم تم
        self.setup_theme()
        
        # ایجاد رابط کاربری
        self.create_widgets()
        
        # شروع بروزرسانی دوره‌ای
        self.update_status()
        
        # تست اتصال
        self.test_connection()
    
    def setup_theme(self):
        """تنظیم تم و استایل"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # رنگ‌ها
        self.bg_color = "#f0f0f0"
        self.accent_color = "#2c3e50"
        self.upload_color = "#27ae60"
        self.download_color = "#3498db"
        
        self.root.configure(bg=self.bg_color)
    
    def create_widgets(self):
        """ایجاد ویجت‌های رابط کاربری"""
        
        # فریم اصلی
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # تنظیم وزن سطرها و ستون‌ها
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        for i in range(8):
            main_frame.rowconfigure(i, weight=0)
        
        # 1. هدر
        header_frame = ttk.Frame(main_frame, relief="solid", padding="10")
        header_frame.grid(row=0, column=0, columnspan=3, pady=(0, 10), sticky=(tk.W, tk.E))
        
        ttk.Label(header_frame, text="📁 IPFS Client", font=("Arial", 20, "bold")).pack(side=tk.LEFT)
        
        # وضعیت اتصال
        self.connection_status = ttk.Label(header_frame, text="● قطع", foreground="red")
        self.connection_status.pack(side=tk.RIGHT, padx=10)
        
        # 2. بخش آپلود
        upload_frame = ttk.LabelFrame(main_frame, text="آپلود فایل", padding="15")
        upload_frame.grid(row=1, column=0, columnspan=2, pady=10, sticky=(tk.W, tk.E))
        upload_frame.columnconfigure(1, weight=1)
        
        # انتخاب فایل
        ttk.Label(upload_frame, text="فایل:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        
        self.file_path_var = tk.StringVar()
        file_entry = ttk.Entry(upload_frame, textvariable=self.file_path_var, width=50)
        file_entry.grid(row=0, column=1, padx=5, pady=5, sticky=(tk.W, tk.E))
        
        browse_btn = ttk.Button(upload_frame, text="انتخاب فایل...", command=self.browse_file)
        browse_btn.grid(row=0, column=2, padx=5, pady=5)
        
        upload_btn = ttk.Button(upload_frame, text="شروع آپلود", 
                               command=self.start_upload, 
                               style="Accent.TButton")
        upload_btn.grid(row=1, column=2, padx=5, pady=10, sticky=tk.E)
        
        # پیشنمایش فایل
        self.file_info_label = ttk.Label(upload_frame, text="هیچ فایلی انتخاب نشده")
        self.file_info_label.grid(row=1, column=0, columnspan=2, padx=5, pady=5, sticky=tk.W)
        
        # 3. بخش دانلود
        download_frame = ttk.LabelFrame(main_frame, text="دانلود فایل", padding="15")
        download_frame.grid(row=2, column=0, columnspan=2, pady=10, sticky=(tk.W, tk.E))
        download_frame.columnconfigure(1, weight=1)
        
        ttk.Label(download_frame, text="CID:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        
        self.cid_var = tk.StringVar()
        cid_entry = ttk.Entry(download_frame, textvariable=self.cid_var, width=50)
        cid_entry.grid(row=0, column=1, padx=5, pady=5, sticky=(tk.W, tk.E))
        
        ttk.Label(download_frame, text="مسیر ذخیره:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        
        self.save_path_var = tk.StringVar(value=os.path.expanduser("~/Downloads"))
        save_entry = ttk.Entry(download_frame, textvariable=self.save_path_var, width=50)
        save_entry.grid(row=1, column=1, padx=5, pady=5, sticky=(tk.W, tk.E))
        
        browse_save_btn = ttk.Button(download_frame, text="انتخاب مسیر...", 
                                    command=self.browse_save_path)
        browse_save_btn.grid(row=1, column=2, padx=5, pady=5)
        
        download_btn = ttk.Button(download_frame, text="شروع دانلود", 
                                 command=self.start_download,
                                 style="Accent.TButton")
        download_btn.grid(row=2, column=2, padx=5, pady=10, sticky=tk.E)
        
        # 4. بخش وضعیت فعال
        active_frame = ttk.LabelFrame(main_frame, text="عملیات‌های فعال", padding="15")
        active_frame.grid(row=1, column=2, rowspan=2, padx=(10, 0), pady=10, sticky=(tk.W, tk.E, tk.N, tk.S))
        active_frame.columnconfigure(0, weight=1)
        active_frame.rowconfigure(1, weight=1)
        
        # درخت نمایش عملیات‌های فعال
        columns = ('operation', 'filename', 'progress', 'status', 'speed')
        self.active_tree = ttk.Treeview(active_frame, columns=columns, show='headings', height=8)
        
        # تعریف ستون‌ها
        self.active_tree.heading('operation', text='عملیات')
        self.active_tree.heading('filename', text='نام فایل')
        self.active_tree.heading('progress', text='پیشرفت')
        self.active_tree.heading('status', text='وضعیت')
        self.active_tree.heading('speed', text='سرعت')
        
        self.active_tree.column('operation', width=80)
        self.active_tree.column('filename', width=150)
        self.active_tree.column('progress', width=100)
        self.active_tree.column('status', width=100)
        self.active_tree.column('speed', width=80)
        
        scrollbar = ttk.Scrollbar(active_frame, orient=tk.VERTICAL, command=self.active_tree.yview)
        self.active_tree.configure(yscrollcommand=scrollbar.set)
        
        self.active_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # دکمه‌های مدیریت
        button_frame = ttk.Frame(active_frame)
        button_frame.grid(row=1, column=0, columnspan=2, pady=10, sticky=tk.E)
        
        ttk.Button(button_frame, text="توقف", command=self.stop_selected).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="توقف همه", command=self.stop_all).pack(side=tk.RIGHT, padx=5)
        
        # 5. لاگ عملیات‌ها
        log_frame = ttk.LabelFrame(main_frame, text="لاگ عملیات‌ها", padding="10")
        log_frame.grid(row=3, column=0, columnspan=3, pady=(10, 0), sticky=(tk.W, tk.E, tk.N, tk.S))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=10, wrap=tk.WORD)
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # دکمه پاک کردن لاگ
        ttk.Button(log_frame, text="پاک کردن لاگ", command=self.clear_log).grid(row=1, column=0, pady=5, sticky=tk.E)
        
        # 6. آمار سیستم
        stats_frame = ttk.Frame(main_frame)
        stats_frame.grid(row=4, column=0, columnspan=3, pady=10, sticky=(tk.W, tk.E))
        
        self.stats_label = ttk.Label(stats_frame, 
                                    text="آماده برای عملیات...",
                                    font=("Arial", 9))
        self.stats_label.pack(side=tk.LEFT)
        
        # تنظیم استایل‌ها
        style = ttk.Style()
        style.configure('Accent.TButton', background=self.accent_color, foreground='white')
        
    def browse_file(self):
        """انتخاب فایل برای آپلود"""
        filepath = filedialog.askopenfilename(
            title="انتخاب فایل برای آپلود",
            filetypes=[
                ("همه فایل‌ها", "*.*"),
                ("تصاویر", "*.jpg *.jpeg *.png *.gif"),
                ("مستندات", "*.pdf *.doc *.docx *.txt"),
                ("ویدیو", "*.mp4 *.avi *.mov")
            ]
        )
        
        if filepath:
            self.file_path_var.set(filepath)
            file_size = os.path.getsize(filepath)
            self.file_info_label.config(
                text=f"فایل: {os.path.basename(filepath)} | اندازه: {self.format_size(file_size)}"
            )
    
    def browse_save_path(self):
        """انتخاب مسیر ذخیره"""
        path = filedialog.askdirectory(
            title="انتخاب مسیر ذخیره",
            initialdir=self.save_path_var.get()
        )
        
        if path:
            self.save_path_var.set(path)
    
    def start_upload(self):
        """شروع آپلود فایل"""
        filepath = self.file_path_var.get()
        
        if not filepath or not os.path.exists(filepath):
            messagebox.showerror("خطا", "لطفا یک فایل معتبر انتخاب کنید")
            return
        
        # شروع آپلود در یک ترد جداگانه
        thread = threading.Thread(target=self.upload_thread, args=(filepath,))
        thread.daemon = True
        thread.start()
        
        self.log_message("آپلود", f"شروع آپلود فایل: {os.path.basename(filepath)}")
    
    def start_download(self):
        """شروع دانلود فایل"""
        cid = self.cid_var.get().strip()
        save_path = self.save_path_var.get()
        
        if not cid:
            messagebox.showerror("خطا", "لطفا CID فایل را وارد کنید")
            return
        
        # شروع دانلود در یک ترد جداگانه
        thread = threading.Thread(target=self.download_thread, args=(cid, save_path))
        thread.daemon = True
        thread.start()
        
        self.log_message("دانلود", f"شروع دانلود با CID: {cid}")
    
    def upload_thread(self, filepath):
        """ترد آپلود فایل"""
        try:
            filename = os.path.basename(filepath)
            
            # شبیه‌سازی آپلود
            import time
            import random
            
            file_size = os.path.getsize(filepath)
            request_id = f"upload_{int(time.time())}_{random.randint(1000, 9999)}"
            
            # افزودن به لیست عملیات‌های فعال
            self.active_uploads[request_id] = {
                'filename': filename,
                'progress': 0,
                'status': 'در حال آپلود',
                'speed': '0 KB/s',
                'size': file_size,
                'start_time': time.time()
            }
            
            # شبیه‌سازی پیشرفت
            for i in range(0, 101, 5):
                time.sleep(0.2)  # شبیه‌سازی تأخیر
                
                # به‌روزرسانی پیشرفت
                self.active_uploads[request_id]['progress'] = i
                self.active_uploads[request_id]['speed'] = f"{random.randint(100, 1000)} KB/s"
                
                if i == 100:
                    self.active_uploads[request_id]['status'] = 'تکمیل شده'
                    self.log_message("آپلود", f"آپلود فایل {filename} تکمیل شد")
                    
                    # حذف پس از 5 ثانیه
                    threading.Timer(5, lambda: self.cleanup_operation(request_id)).start()
            
        except Exception as e:
            self.log_message("خطا", f"خطا در آپلود: {str(e)}")
    
    def download_thread(self, cid, save_path):
        """ترد دانلود فایل"""
        try:
            # شبیه‌سازی دانلود
            import time
            import random
            
            request_id = f"download_{int(time.time())}_{random.randint(1000, 9999)}"
            filename = f"file_{cid[:8]}.bin"  # نام فرضی
            
            # افزودن به لیست عملیات‌های فعال
            self.active_downloads[request_id] = {
                'filename': filename,
                'progress': 0,
                'status': 'در حال دانلود',
                'speed': '0 KB/s',
                'size': random.randint(1024*1024, 50*1024*1024),  # 1MB تا 50MB
                'cid': cid,
                'start_time': time.time()
            }
            
            # شبیه‌سازی پیشرفت
            for i in range(0, 101, 2):
                time.sleep(0.3)  # شبیه‌سازی تأخیر
                
                # به‌روزرسانی پیشرفت
                self.active_downloads[request_id]['progress'] = i
                self.active_downloads[request_id]['speed'] = f"{random.randint(500, 2000)} KB/s"
                
                if i == 100:
                    self.active_downloads[request_id]['status'] = 'تکمیل شده'
                    
                    # ذخیره فایل (شبیه‌سازی)
                    import shutil
                    fake_file = os.path.join(save_path, filename)
                    with open(fake_file, 'wb') as f:
                        f.write(b'0' * 1024)  # فایل 1KB خالی
                    
                    self.log_message("دانلود", f"دانلود فایل به {fake_file} تکمیل شد")
                    
                    # حذف پس از 5 ثانیه
                    threading.Timer(5, lambda: self.cleanup_operation(request_id)).start()
            
        except Exception as e:
            self.log_message("خطا", f"خطا در دانلود: {str(e)}")
    
    def update_status(self):
        """بروزرسانی دوره‌ای وضعیت"""
        try:
            # پاک کردن درخت فعلی
            for item in self.active_tree.get_children():
                self.active_tree.delete(item)
            
            # افزودن آپلودهای فعال
            for req_id, data in self.active_uploads.items():
                progress_bar = f"{data['progress']}%"
                self.active_tree.insert('', 'end', 
                    values=('📤 آپلود', data['filename'], progress_bar, 
                           data['status'], data['speed']),
                    tags=('upload',))
            
            # افزودن دانلودهای فعال
            for req_id, data in self.active_downloads.items():
                progress_bar = f"{data['progress']}%"
                self.active_tree.insert('', 'end',
                    values=('📥 دانلود', data['filename'], progress_bar,
                           data['status'], data['speed']),
                    tags=('download',))
            
            # بروزرسانی آمار
            total_active = len(self.active_uploads) + len(self.active_downloads)
            self.stats_label.config(
                text=f"عملیات فعال: {total_active} | آپلود: {len(self.active_uploads)} | دانلود: {len(self.active_downloads)}"
            )
            
            # بروزرسانی وضعیت اتصال
            if self.test_connection_silent():
                self.connection_status.config(text="● متصل", foreground="green")
            else:
                self.connection_status.config(text="● قطع", foreground="red")
            
        except Exception as e:
            print(f"خطا در بروزرسانی وضعیت: {e}")
        
        # زمان‌بندی بروزرسانی بعدی
        self.root.after(1000, self.update_status)
    
    def cleanup_operation(self, request_id):
        """پاکسازی عملیات تکمیل شده"""
        if request_id in self.active_uploads:
            del self.active_uploads[request_id]
        elif request_id in self.active_downloads:
            del self.active_downloads[request_id]
    
    def stop_selected(self):
        """توقف عملیات انتخاب شده"""
        selection = self.active_tree.selection()
        if selection:
            item = self.active_tree.item(selection[0])
            operation_type = item['values'][0]
            filename = item['values'][1]
            
            # پیدا کردن و توقف عملیات
            # در اینجا باید کد واقعی توقف عملیات نوشته شود
            
            self.log_message("توقف", f"عملیات {operation_type} برای فایل {filename} متوقف شد")
    
    def stop_all(self):
        """توقف همه عملیات‌ها"""
        if messagebox.askyesno("تایید", "آیا از توقف همه عملیات‌ها مطمئنید؟"):
            self.active_uploads.clear()
            self.active_downloads.clear()
            self.log_message("سیستم", "همه عملیات‌ها متوقف شدند")
    
    def log_message(self, category, message):
        """افزودن پیام به لاگ"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] [{category}] {message}\n"
        
        self.log_text.insert(tk.END, formatted_message)
        self.log_text.see(tk.END)
        
        # رنگ‌بندی بر اساس دسته
        if category == "خطا":
            self.log_text.tag_add("error", f"end-2l linestart", f"end-2l lineend")
            self.log_text.tag_config("error", foreground="red")
        elif category == "آپلود":
            self.log_text.tag_add("upload", f"end-2l linestart", f"end-2l lineend")
            self.log_text.tag_config("upload", foreground="green")
        elif category == "دانلود":
            self.log_text.tag_add("download", f"end-2l linestart", f"end-2l lineend")
            self.log_text.tag_config("download", foreground="blue")
    
    def clear_log(self):
        """پاک کردن لاگ"""
        self.log_text.delete(1.0, tk.END)
    
    def test_connection(self):
        """تست اتصال به سرور"""
        if self.test_connection_silent():
            messagebox.showinfo("اتصال", "اتصال به سرور IPFS برقرار است")
        else:
            messagebox.showwarning("اتصال", "اتصال به سرور IPFS برقرار نیست")
    
    def test_connection_silent(self):
        """تست اتصال بدون نمایش پیام"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((self.server_host, self.server_port))
            sock.close()
            return result == 0
        except:
            return False
    
    def format_size(self, size):
        """فرمت‌دهی اندازه فایل"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"

def main():
    """تابع اصلی"""
    root = tk.Tk()
    app = IPFSClientGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
