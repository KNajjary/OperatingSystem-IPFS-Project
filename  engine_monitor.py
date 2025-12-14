# engine_monitor.py
import tkinter as tk
from tkinter import ttk
import socket
import struct
import threading
import queue
import json
from datetime import datetime
import psutil
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import numpy as np

class CEngineMonitor:
    """داشبورد مانیتورینگ برای c_engine"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("IPFS c_engine Monitor - کنترل وضعیت هسته")
        self.root.geometry("1200x800")
        
        # اتصال به سوکت c_engine
        self.socket_path = "/tmp/cengine.sock"
        
        # داده‌های مانیتورینگ
        self.engine_stats = {
            'active_connections': 0,
            'total_uploads': 0,
            'total_downloads': 0,
            'memory_usage': 0,
            'cpu_usage': 0,
            'chunks_processed': 0,
            'errors': 0,
            'uptime': 0
        }
        
        # تاریخچه‌ها برای نمودارها
        self.cpu_history = []
        self.memory_history = []
        self.network_history = []
        
        # عملیات‌های جاری
        self.current_operations = {}
        
        # صف برای ارتباط
        self.message_queue = queue.Queue()
        
        # تنظیم رابط کاربری
        self.setup_ui()
        
        # شروع مانیتورینگ
        self.start_monitoring()
        
    def setup_ui(self):
        """تنظیم رابط کاربری"""
        
        # ایجاد نوار بالایی
        header_frame = ttk.Frame(self.root, padding="10")
        header_frame.grid(row=0, column=0, columnspan=3, sticky=(tk.W, tk.E))
        
        ttk.Label(header_frame, text="🚀 IPFS c_engine Monitor", 
                 font=("Arial", 18, "bold")).pack(side=tk.LEFT)
        
        self.status_label = ttk.Label(header_frame, text="● در حال اجرا", 
                                     foreground="green")
        self.status_label.pack(side=tk.RIGHT, padx=20)
        
        # بخش آمار لحظه‌ای
        stats_frame = ttk.LabelFrame(self.root, text="آمار لحظه‌ای", padding="15")
        stats_frame.grid(row=1, column=0, columnspan=3, padx=10, pady=10, sticky=(tk.W, tk.E))
        
        # ایجاد ویجت‌های آمار
        self.create_stats_widgets(stats_frame)
        
        # نمودارهای عملکرد
        charts_frame = ttk.Frame(self.root)
        charts_frame.grid(row=2, column=0, columnspan=2, padx=10, pady=10, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self.create_charts(charts_frame)
        
        # لیست عملیات‌های جاری
        operations_frame = ttk.LabelFrame(self.root, text="عملیات‌های جاری", padding="10")
        operations_frame.grid(row=2, column=2, padx=10, pady=10, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self.create_operations_list(operations_frame)
        
        # لاگ سیستم
        log_frame = ttk.LabelFrame(self.root, text="لاگ سیستم", padding="10")
        log_frame.grid(row=3, column=0, columnspan=3, padx=10, pady=10, sticky=(tk.W, tk.E))
        
        self.create_system_log(log_frame)
        
        # دکمه‌های کنترل
        control_frame = ttk.Frame(self.root)
        control_frame.grid(row=4, column=0, columnspan=3, pady=10)
        
        self.create_control_buttons(control_frame)
        
        # تنظیم وزن‌ها
        self.root.columnconfigure(0, weight=1)
        self.root.columnconfigure(1, weight=1)
        self.root.columnconfigure(2, weight=1)
        self.root.rowconfigure(2, weight=1)
        self.root.rowconfigure(3, weight=0)
        
    def create_stats_widgets(self, parent):
        """ایجاد ویجت‌های نمایش آمار"""
        
        # شبکه‌ای از آمارها
        stats_grid = ttk.Frame(parent)
        stats_grid.pack(fill=tk.BOTH, expand=True)
        
        # تعریف آمارها
        stats_info = [
            ("اتصالات فعال:", "active_connections", "connections"),
            ("آپلودهای کل:", "total_uploads", "uploads"),
            ("دانلودهای کل:", "total_downloads", "downloads"),
            ("چانک‌های پردازش شده:", "chunks_processed", "chunks"),
            ("مصرف CPU:", "cpu_usage", "cpu"),
            ("مصرف حافظه:", "memory_usage", "memory"),
            ("خطاها:", "errors", "errors"),
            ("زمان فعالیت:", "uptime", "uptime")
        ]
        
        row, col = 0, 0
        for label_text, stat_key, icon in stats_info:
            frame = ttk.Frame(stats_grid, relief="solid", padding="10")
            frame.grid(row=row, column=col, padx=5, pady=5, sticky=(tk.W, tk.E, tk.N, tk.S))
            
            # آیکون
            icon_label = ttk.Label(frame, text=self.get_icon(icon), font=("Arial", 14))
            icon_label.grid(row=0, column=0, rowspan=2, padx=(0, 10))
            
            # برچسب
            ttk.Label(frame, text=label_text, font=("Arial", 9)).grid(row=0, column=1, sticky=tk.W)
            
            # مقدار
            value_var = tk.StringVar(value="0")
            value_label = ttk.Label(frame, textvariable=value_var, 
                                   font=("Arial", 16, "bold"))
            value_label.grid(row=1, column=1, sticky=tk.W)
            
            # ذخیره متغیر برای به‌روزرسانی
            setattr(self, f"{stat_key}_var", value_var)
            
            # واحد
            unit_label = ttk.Label(frame, text=self.get_unit(stat_key))
            unit_label.grid(row=1, column=2, sticky=tk.W, padx=(5, 0))
            
            col += 1
            if col >= 4:
                col = 0
                row += 1
        
        # تنظیم وزن ستون‌ها
        for i in range(4):
            stats_grid.columnconfigure(i, weight=1)
    
    def create_charts(self, parent):
        """ایجاد نمودارهای عملکرد"""
        
        # فریم برای نمودارها
        charts_container = ttk.Frame(parent)
        charts_container.pack(fill=tk.BOTH, expand=True)
        
        # نمودار CPU
        cpu_frame = ttk.LabelFrame(charts_container, text="مصرف CPU")
        cpu_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.cpu_fig, self.cpu_ax = plt.subplots(figsize=(4, 3))
        self.cpu_ax.set_title("CPU Usage (%)")
        self.cpu_ax.set_ylim(0, 100)
        self.cpu_line, = self.cpu_ax.plot([], [], 'r-')
        
        self.cpu_canvas = FigureCanvasTkAgg(self.cpu_fig, cpu_frame)
        self.cpu_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # نمودار حافظه
        mem_frame = ttk.LabelFrame(charts_container, text="مصرف حافظه")
        mem_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.mem_fig, self.mem_ax = plt.subplots(figsize=(4, 3))
        self.mem_ax.set_title("Memory Usage (MB)")
        self.mem_line, = self.mem_ax.plot([], [], 'b-')
        
        self.mem_canvas = FigureCanvasTkAgg(self.mem_fig, mem_frame)
        self.mem_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    
    def create_operations_list(self, parent):
        """ایجاد لیست عملیات‌های جاری"""
        
        # درخت نمایش عملیات‌ها
        columns = ('id', 'type', 'filename', 'progress', 'speed', 'duration')
        self.ops_tree = ttk.Treeview(parent, columns=columns, show='headings', height=15)
        
        # تعریف ستون‌ها
        self.ops_tree.heading('id', text='شناسه')
        self.ops_tree.heading('type', text='نوع')
        self.ops_tree.heading('filename', text='فایل')
        self.ops_tree.heading('progress', text='پیشرفت')
        self.ops_tree.heading('speed', text='سرعت')
        self.ops_tree.heading('duration', text='زمان')
        
        self.ops_tree.column('id', width=80)
        self.ops_tree.column('type', width=80)
        self.ops_tree.column('filename', width=150)
        self.ops_tree.column('progress', width=100)
        self.ops_tree.column('speed', width=100)
        self.ops_tree.column('duration', width=100)
        
        scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=self.ops_tree.yview)
        self.ops_tree.configure(yscrollcommand=scrollbar.set)
        
        self.ops_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    
    def create_system_log(self, parent):
        """ایجاد بخش لاگ سیستم"""
        
        self.log_text = tk.Text(parent, height=8, wrap=tk.WORD, bg='black', fg='white')
        scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # تگ‌های رنگ
        self.log_text.tag_config('INFO', foreground='cyan')
        self.log_text.tag_config('WARNING', foreground='yellow')
        self.log_text.tag_config('ERROR', foreground='red')
        self.log_text.tag_config('SUCCESS', foreground='green')
    
    def create_control_buttons(self, parent):
        """ایجاد دکمه‌های کنترل"""
        
        ttk.Button(parent, text="بارگذاری مجدد", 
                  command=self.reload_engine).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(parent, text="پاکسازی حافظه", 
                  command=self.clear_memory).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(parent, text="خروجی دیباگ", 
                  command=self.generate_debug).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(parent, text="پاک کردن لاگ", 
                  command=self.clear_log).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(parent, text="توقف c_engine", 
                  command=self.stop_engine, 
                  style="Danger.TButton").pack(side=tk.RIGHT, padx=5)
        
        # استایل دکمه خطر
        style = ttk.Style()
        style.configure('Danger.TButton', background='#e74c3c', foreground='white')
    
    def start_monitoring(self):
        """شروع مانیتورینگ"""
        
        # شروع ترد مانیتورینگ
        self.monitor_thread = threading.Thread(target=self.monitor_engine)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        # شروع بروزرسانی UI
        self.update_ui()
        
        # شروع بروزرسانی نمودارها
        self.update_charts()
        
        self.log_message("INFO", "سیستم مانیتورینگ راه‌اندازی شد")
    
    def monitor_engine(self):
        """مانیتورینگ c_engine"""
        while True:
            try:
                # شبیه‌سازی داده‌های مانیتورینگ
                self.simulate_engine_data()
                
                # ارسال به صف برای UI
                self.message_queue.put(self.engine_stats.copy())
                
                # خواب
                import time
                time.sleep(1)
                
            except Exception as e:
                self.log_message("ERROR", f"خطا در مانیتورینگ: {str(e)}")
                time.sleep(5)
    
    def simulate_engine_data(self):
        """شبیه‌سازی داده‌های c_engine"""
        import random
        import time
        
        # شبیه‌سازی داده‌های واقعی
        self.engine_stats['cpu_usage'] = psutil.cpu_percent()
        self.engine_stats['memory_usage'] = psutil.virtual_memory().percent
        
        # افزایش سایر آمارها
        self.engine_stats['active_connections'] = random.randint(0, 10)
        self.engine_stats['chunks_processed'] += random.randint(0, 100)
        self.engine_stats['uptime'] += 1
        
        # شبیه‌سازی عملیات‌های تصادفی
        if random.random() > 0.7:  # 30% احتمال
            op_type = random.choice(['upload', 'download'])
            op_id = f"op_{int(time.time())}_{random.randint(1000, 9999)}"
            
            self.current_operations[op_id] = {
                'type': op_type,
                'filename': f"file_{random.randint(1, 1000)}.dat",
                'progress': 0,
                'speed': f"{random.randint(100, 1000)} KB/s",
                'start_time': time.time(),
                'duration': '00:00'
            }
        
        # به‌روزرسانی پیشرفت عملیات‌های موجود
        for op_id in list(self.current_operations.keys()):
            op = self.current_operations[op_id]
            op['progress'] += random.randint(1, 5)
            
            # محاسبه مدت زمان
            duration_sec = int(time.time() - op['start_time'])
            op['duration'] = f"{duration_sec//60:02d}:{duration_sec%60:02d}"
            
            # حذف عملیات‌های تکمیل شده
            if op['progress'] >= 100:
                if op['type'] == 'upload':
                    self.engine_stats['total_uploads'] += 1
                else:
                    self.engine_stats['total_downloads'] += 1
                
                del self.current_operations[op_id]
    
    def update_ui(self):
        """بروزرسانی رابط کاربری"""
        try:
            # بررسی صف برای داده‌های جدید
            while not self.message_queue.empty():
                new_stats = self.message_queue.get_nowait()
                self.engine_stats.update(new_stats)
            
            # به‌روزرسانی مقادیر آمار
            for key, value in self.engine_stats.items():
                var_name = f"{key}_var"
                if hasattr(self, var_name):
                    getattr(self, var_name).set(self.format_stat(key, value))
            
            # به‌روزرسانی لیست عملیات‌ها
            self.update_operations_list()
            
            # به‌روزرسانی تاریخچه برای نمودارها
            self.cpu_history.append(self.engine_stats['cpu_usage'])
            self.memory_history.append(self.engine_stats['memory_usage'])
            
            # محدود کردن اندازه تاریخچه
            if len(self.cpu_history) > 50:
                self.cpu_history.pop(0)
                self.memory_history.pop(0)
            
        except Exception as e:
            print(f"خطا در بروزرسانی UI: {e}")
        
        # زمان‌بندی بروزرسانی بعدی
        self.root.after(1000, self.update_ui)
    
    def update_charts(self):
        """بروزرسانی نمودارها"""
        if len(self.cpu_history) > 0:
            # بروزرسانی نمودار CPU
            self.cpu_line.set_data(range(len(self.cpu_history)), self.cpu_history)
            self.cpu_ax.set_xlim(0, len(self.cpu_history))
            self.cpu_ax.relim()
            self.cpu_ax.autoscale_view()
            self.cpu_canvas.draw()
            
            # بروزرسانی نمودار حافظه
            self.mem_line.set_data(range(len(self.memory_history)), self.memory_history)
            self.mem_ax.set_xlim(0, len(self.memory_history))
            self.mem_ax.relim()
            self.mem_ax.autoscale_view()
            self.mem_canvas.draw()
        
        # زمان‌بندی بروزرسانی بعدی
        self.root.after(2000, self.update_charts)
    
    def update_operations_list(self):
        """بروزرسانی لیست عملیات‌های جاری"""
        # پاک کردن لیست فعلی
        for item in self.ops_tree.get_children():
            self.ops_tree.delete(item)
        
        # افزودن عملیات‌های جدید
        for op_id, op_data in self.current_operations.items():
            icon = "📤" if op_data['type'] == 'upload' else "📥"
            self.ops_tree.insert('', 'end', 
                values=(op_id[-8:], f"{icon} {op_data['type']}", 
                       op_data['filename'], f"{op_data['progress']}%",
                       op_data['speed'], op_data['duration']))
    
    def log_message(self, level, message):
        """افزودن پیام به لاگ سیستم"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted = f"[{timestamp}] [{level}] {message}\n"
        
        self.log_text.insert(tk.END, formatted, level)
        self.log_text.see(tk.END)
        
        # محدود کردن اندازه لاگ
        if self.log_text.index('end-1c').split('.')[0] > '100':
            self.log_text.delete(1.0, '2.0')
    
    def clear_log(self):
        """پاک کردن لاگ"""
        self.log_text.delete(1.0, tk.END)
    
    def reload_engine(self):
        """بارگذاری مجدد c_engine"""
        self.log_message("INFO", "درخواست بارگذاری مجدد c_engine")
        # در اینجا کد واقعی بارگذاری مجدد
        messagebox.showinfo("اطلاع", "درخواست بارگذاری مجدد ارسال شد")
    
    def clear_memory(self):
        """پاکسازی حافظه"""
        self.log_message("INFO", "درخواست پاکسازی حافظه")
        # در اینجا کد واقعی پاکسازی حافظه
        messagebox.showinfo("اطلاع", "حافظه پاکسازی شد")
    
    def generate_debug(self):
        """تولید خروجی دیباگ"""
        import json
        debug_info = {
            'timestamp': datetime.now().isoformat(),
            'stats': self.engine_stats,
            'operations': len(self.current_operations),
            'system': {
                'cpu': psutil.cpu_percent(),
                'memory': psutil.virtual_memory()._asdict(),
                'disk': psutil.disk_usage('/')._asdict()
            }
        }
        
        filename = f"debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(debug_info, f, indent=2)
        
        self.log_message("SUCCESS", f"فایل دیباگ ذخیره شد: {filename}")
        messagebox.showinfo("اطلاع", f"فایل دیباگ ذخیره شد:\n{filename}")
    
    def stop_engine(self):
        """توقف c_engine"""
        if messagebox.askyesno("تایید", "آیا از توقف c_engine مطمئنید؟"):
            self.log_message("WARNING", "درخواست توقف c_engine")
            # در اینجا کد واقعی توقف
            self.status_label.config(text="● متوقف شده", foreground="red")
            messagebox.showinfo("اطلاع", "c_engine متوقف شد")
    
    def format_stat(self, key, value):
        """فرمت‌دهی مقدار آمار"""
        if key == 'cpu_usage':
            return f"{value:.1f}"
        elif key == 'memory_usage':
            return f"{value:.1f}"
        elif key == 'uptime':
            return self.format_uptime(value)
        else:
            return str(value)
    
    def format_uptime(self, seconds):
        """فرمت‌دهی زمان فعالیت"""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    
    def get_icon(self, icon_type):
        """دریافت آیکون مناسب"""
        icons = {
            'connections': '🔗',
            'uploads': '📤',
            'downloads': '📥',
            'chunks': '🧩',
            'cpu': '⚡',
            'memory': '💾',
            'errors': '⚠️',
            'uptime': '⏱️'
        }
        return icons.get(icon_type, '📊')
    
    def get_unit(self, stat_key):
        """دریافت واحد مناسب"""
        units = {
            'cpu_usage': '%',
            'memory_usage': '%',
            'uptime': ''
        }
        return units.get(stat_key, '')

def main():
    """تابع اصلی"""
    root = tk.Tk()
    app = CEngineMonitor(root)
    root.mainloop()

if __name__ == "__main__":
    main()