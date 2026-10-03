import tkinter as tk
from tkinter import ttk
import multiprocessing
import time


def simulate_data_transfer(update_queue, file_size):
    """子进程模拟数据传输"""
    sent_size = 0
    while sent_size < file_size:
        time.sleep(0.1)  # 模拟传输延迟
        sent_size += 10_000_000  # 每次增加 10 MB
        update_queue.put(sent_size)  # 将当前进度放入队列


def update_ui(root, progress_label, progress_line, update_queue, file_size):
    """主进程更新 UI"""
    try:
        sent_size = update_queue.get_nowait()  # 非阻塞获取更新
        sent_size_mb = sent_size / 1_000_000
        progress_value = (sent_size / file_size) * 100
        progress_line["value"] = progress_value
        progress_label["text"] = f"檔案傳輸進度：{sent_size_mb:.2f} MB"

        if sent_size >= file_size:
            progress_label["text"] = f"檔案傳輸完成，總大小：{file_size / 1_000_000:.2f} MB"
            return
    except multiprocessing.queues.Empty:
        pass

    root.after(100, update_ui, root, progress_label, progress_line, update_queue, file_size)


def main():
    file_size = 100_000_000  # 100 MB
    update_queue = multiprocessing.Queue()

    # 创建子进程
    process = multiprocessing.Process(target=simulate_data_transfer, args=(update_queue, file_size))
    process.start()

    # Tkinter 主进程
    root = tk.Tk()
    root.title("檔案傳輸進度")

    progress_label = tk.Label(root, text="檔案傳輸進度：0.00 MB")
    progress_label.pack(pady=5)

    progress_line = ttk.Progressbar(root, orient="horizontal", length=250, mode="determinate")
    progress_line.pack(pady=5)

    # 开始更新 UI
    root.after(100, update_ui, root, progress_label, progress_line, update_queue, file_size)
    root.mainloop()

    process.join()


if __name__ == "__main__":
    main()
