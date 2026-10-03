import tkinter as tk
from tkinter import ttk
import time

class ProgressBarUI:
    def __init__(self, total):
        self.total = total
        self.current = 0
        
        # 建立主視窗
        self.root = tk.Tk()
        self.root.title("Progress Bar")
        
        # 標籤
        self.label = tk.Label(self.root, text="進度: 0/{}".format(self.total))
        self.label.pack(pady=10)
        
        # 進度條
        self.progress = ttk.Progressbar(self.root, orient="horizontal", length=300, mode="determinate")
        self.progress.pack(pady=10)
        self.progress["maximum"] = self.total
        
    def update(self, step=1):
        self.current += step
        self.progress["value"] = self.current
        self.label.config(text="進度: {}/{}".format(self.current, self.total))
        self.root.update_idletasks()
        
    def close(self):
        self.root.destroy()

# 模擬進度條
def main():
    total_steps = 100
    ui = ProgressBarUI(total_steps)
    
    for i in range(total_steps):
        time.sleep(0.05)  # 模擬任務執行
        ui.update(1)
    
    time.sleep(1)  # 暫停顯示進度完成
    ui.close()

if __name__ == "__main__":
    main()
