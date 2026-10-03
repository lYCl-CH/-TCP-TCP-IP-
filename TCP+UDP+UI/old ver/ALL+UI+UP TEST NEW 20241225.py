import socket
import time
import tkinter as tk
from tkinter import filedialog, messagebox,ttk,scrolledtext  
import hashlib
import os
import threading
import queue

MB = 0
def on_client_click():
    
    listening_port = 60000  # 必須與廣播端的埠號相同
    threading.Thread(target=udp_receive_ip_and_port, args=(listening_port,), daemon=True).start()
  
    


def run_client(ip, port, save_directory):
 update_log("啟動客戶端...")
 TCP_CLIENT(ip, port, save_directory)
#-------
def udp_receive_ip_and_port(port, buffer_size=1024):
    """
    接收區域網路的 UDP 廣播訊息，解析 IP 和埠號。

    :param port: 要監聽的埠號
    :param buffer_size: 接收資料的緩衝大小 (預設為 1024 bytes)
    """
    ip_info = None  # 初始化變數，避免作用域問題

    # 建立 UDP Socket
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_socket.bind(("", port))

    update_log(f"正在監聽 UDP 廣播訊息 (埠: {port})...")

    try:
        while True:
            # 接收來自廣播的訊息
            message, address = udp_socket.recvfrom(buffer_size)
            decoded_message = message.decode('utf-8')
            update_log(f"接收到訊息: {decoded_message} 來自: {address}")
            
            # 回傳自己的 IP 給發送端
            response_message = f"My IP is: {socket.gethostbyname(socket.gethostname())}"
            udp_socket.sendto(response_message.encode('utf-8'), address)
            update_log(f"回傳訊息: {response_message} 來自: {socket.gethostname()}")
            
            ip_info = decoded_message  # 保存接收到的訊息
            
            # 如果訊息包含 IP 和埠號，提取並啟動執行緒
            if "IP: " in ip_info and "Port: " in ip_info:
                ip = ip_info.split("IP: ")[1].split(",")[0].strip()
                port_str = ip_info.split("Port: ")[1].strip()
                target_port = int(port_str)
                update_log(f"提取的 IP: {ip}, 埠號: {target_port}")

                save_directory = "./received_files"
                if not os.path.exists(save_directory):
                    os.makedirs(save_directory)

                threading.Thread(target=run_client, args=(ip, target_port, save_directory), daemon=True).start()
                update_log("執行緒已啟動，開始處理 run_client")
                
            else:
                update_log("訊息中未包含 IP 或埠號")
    except KeyboardInterrupt:
        update_log("停止接收廣播訊息")
    finally:
        udp_socket.close()
        update_log("Socket 已關閉")
    
    # 開啟一個子線程執行客戶端
    


def TCP_CLIENT(HOST, PORT, save_directory):
    global update_interval ,update_queue,stop_flag,received_size
    client_socket = None
    retry_count = 5  # 最大重試次數
    retry_interval = 3  # 每次重試的間隔（秒）

    for attempt in range(1, retry_count + 1):
        try:
            # 嘗試建立連接
            
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFFER_SIZE)  # 發送緩衝區
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)  # 接收緩衝區
  
            client_socket.settimeout(15)
            client_socket.connect((HOST, PORT))  # 嘗試連接伺服器
            update_log(f"已連接到伺服器 {HOST}:{PORT}")
            break  # 成功連接後跳出循環

        except ConnectionRefusedError:
            update_log(f"無法連接到伺服器 {HOST}:{PORT} (重試次數: {attempt}/{retry_count})，請確認伺服器是否啟動並且可用。")
            if attempt < retry_count:
                update_log(f"等待 {retry_interval} 秒後重試...")
                time.sleep(retry_interval)
            else:
                update_log("已達最大重試次數，連接失敗。")
                return  # 超過最大重試次數，退出程式

        except Exception as e:
            update_log(f"發生錯誤: {e}")
            return

    try:
        while True:
            metadata = b""
            while not metadata.endswith(b"\n"):  # 確保接收到完整的元數據
                metadata += client_socket.recv(BUFFER_SIZE)
            try:
                metadata = metadata.decode('utf-8').strip()
            except UnicodeDecodeError:
                update_log("接收到無法解碼的元數據")
                return

            if metadata == "END_OF_TRANSMISSION":  # 檢查結束訊息
                update_log("所有檔案接收完成。")
                break  # 結束循環
            if metadata == "END_OF_FILE":  
                update_log("檔案傳輸完成標記接收，忽略該訊息。")
                continue  # 忽略 END_OF_FILE 訊息

            if "|" not in metadata:
                update_log(f"無效的 metadata：{metadata}")
                continue  # 忽略不正確的 metadata

            file_name, file_size, file_md5 = metadata.split('|')
            file_size = int(file_size)
            file_size_mb=file_size/1_000_000
            save_path = os.path.join(save_directory, file_name)
            update_log(f"準備接收檔案：{file_name}，大小：{file_size_mb} MB，MD5：{file_md5}")

            # 接收檔案數據
            received_size = 0
            update_queue = queue.Queue()
            mbytes =received_size
             # 這裡設置了 update_interval
            MAX_RECEIVE_RETRIES = 3
            # 確保所有需要的參數都正確傳遞

            
            with open(save_path, "wb") as f:
                stop_flag=False
                threading.Thread(target=mbps, args=( update_queue, file_size), daemon=True).start()
                while received_size < file_size:
                    data = client_socket.recv(BUFFER_SIZE)
                    if b"END_OF_FILE" in data:  # 處理 END_OF_FILE 訊息
                        data = data.replace(b"END_OF_FILE", b"")
                        f.write(data)
                        break
                    
 ##                  threading.Thread(target=progress_thread, args=(update_queue, file_size), daemon=True).start()
                    

                    update_queue.put(received_size) 
                    f.write(data)
                    received_size += len(data)
                    MB = len(data)
##                    print(received_size,"|")
                    
            received_md5 = calculate_md5(save_path)
            if received_md5 == file_md5:
                update_log(f"檔案 '{file_name}' 接收成功且 MD5 驗證通過。")
                stop_flag=True
                client_socket.sendall(b"ACK\n")
                
            else:
                update_log(f"檔案 '{file_name}' 驗證失敗，MD5 不匹配！")
                stop_flag=True
                os.remove(save_path)
                client_socket.sendall(b"RESEND\n")
                    

    except Exception as e:
        update_log(f"發生錯誤: {e}")
    finally:
        if client_socket:
            client_socket.close()
        update_log("連接已關閉。")



#------
def on_server_click():
   
    file_paths = select_files()
    update_log(f"選擇的檔案：{file_paths}")

    # 獲取本機 IP
    ip = socket.gethostbyname(socket.gethostname())
    port = 60000 
    threading.Thread(target=udp_broadcast_ip_and_port, args=(ip, port, interval:=3,file_paths), daemon=True).start()
     # 設定埠號
    
    
##    TCP_SERVER(ip, port, file_paths, progress)
    
    # 開啟一個子線程執行伺服器
    
def run_server(ip, port, file_paths):
    update_log("啟動伺服器...")
    TCP_SERVER(ip, port, file_paths)
        
def udp_broadcast_ip_and_port(ip, port, interval,file_paths):
    """
    在區域網路中透過 UDP 廣播 IP 和埠號資訊。

    :param ip: 要廣播的 IP 位址
    :param port: 要廣播的埠號
    :param interval: 廣播的時間間隔 (秒)
    """
    # 建立 UDP Socket
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # 啟用廣播功能
    udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    # 廣播位址
    broadcast_address = "255.255.255.255"

    try:
        while True:
            # 廣播訊息內容
            message = f"IP: {ip}, Port: {port}"
            udp_socket.sendto(message.encode('utf-8'), (broadcast_address, port))
            update_log(f"已廣播: {message}")
            
            # 等待指定的間隔時間
            time.sleep(interval)
            
            # 接收回應的訊息 (自己的IP)
            udp_socket.settimeout(1)  # 設置超時
            try:
                response, address = udp_socket.recvfrom(BUFFER_SIZE)
                #response_message =
                update_log(f"接收到回應: {response.decode('utf-8')} 來自: {address}")
                threading.Thread(target=run_server, args=(ip, port, file_paths), daemon=True).start()
                break  # 收到回應後停止廣播
            except socket.timeout:
                continue  # 如果沒有收到回應，繼續廣播
    except KeyboardInterrupt:
        update_log("廣播停止")
    finally:
        udp_socket.close()
        
def TCP_SERVER(HOST, PORT, file_paths):
    global update_interval,sent_size,stop_flag,update_queue ,received_size
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)  # 接收緩衝區
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFFER_SIZE)
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        update_log(f"伺服器正在 {HOST}:{PORT} 等待客戶端連接...")

        while True:  # 持續等待新的客戶端
            client_socket, client_address = server_socket.accept()
            update_log(f"客戶端 {client_address} 已連接。")

            try:
                for file_path in file_paths:
                    while True:  # 重傳機制
                        file_name = os.path.basename(file_path)
                        file_size = os.path.getsize(file_path)
                        file_md5 = calculate_md5(file_path)
                        metadata = f"{file_name}|{file_size}|{file_md5}\n"
                        client_socket.sendall(metadata.encode('utf-8'))

                        # 初始化進度條
                        sent_size = 0
                        received_size = sent_size
                        update_queue = queue.Queue()
                        update_interval = 0.5
##                        mbps(update_queue, file_size)
##                        progress_thread(update_queue, file_size)
                
                        with open(file_path, "rb") as f:
                            stop_flag=False
                            threading.Thread(target=mbps, args=( update_queue, file_size), daemon=True).start()
##                            threading.Thread(target=progress_thread, args=(update_queue, file_size), daemon=True).start()
                            while chunk := f.read(BUFFER_SIZE):  # 每次讀取 BUFFER_SIZE bytes
                                client_socket.sendall(chunk)
                                sent_size += len(chunk)
                                MB=len(chunk)
                                update_queue.put(sent_size)
                                 
                                
                                

                                
 
                                # 更新進度條，傳遞 received_size 和 file_size
                        update_log(f"檔案 '{file_name}' 已成功傳送，等待客戶端確認...")

                        # 等待客戶端的回應
                        client_socket.sendall(b"END_OF_FILE\n")

                        # 等待客戶端的回應
                        server_response = client_socket.recv(BUFFER_SIZE).decode('utf-8').strip()
                        if server_response == "ACK":
                            update_log(f"客戶端確認檔案 '{file_name}' 接收成功。")
                            stop_flag=True
                            break  # 檔案成功接收，跳出重傳循環
                        elif server_response == "RESEND":
                            update_log(f"客戶端要求重傳檔案 '{file_name}'，正在重傳...")
                            stop_flag=True
                            continue  # 重新傳送該檔案
                        else:
                            update_log(f"未知的回應：{server_response}")
                            break  # 如果收到未知訊息，跳出循環

                # 所有檔案傳送完畢，發送 END_OF_TRANSMISSION 訊息6
                client_socket.sendall(b"END_OF_TRANSMISSION\n")
                update_log("所有檔案已傳送完成。")
                stop_flag=True
                break  # 跳出伺服器的處理循環

            except Exception as e:
                update_log(f"傳送檔案時發生錯誤：{e}")
                continue
            finally:
                client_socket.close()
                update_log(f"與客戶端 {client_address} 的連線已關閉。")
        

def calculate_md5(file_path):
    """計算檔案的 MD5 值"""
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(BUFFER_SIZE):  # 減少讀取緩衝區的大小，確保效率
            md5_hash.update(chunk)
    return md5_hash.hexdigest()




##-----

#--
def select_files():
    """讓使用者選擇要傳送的檔案"""
    file_paths = filedialog.askopenfilenames(
        title="選擇要傳送的檔案",
        filetypes=[("所有檔案", "*.*")]
    )
    if not file_paths:
        messagebox.showwarning("未選擇檔案", "您尚未選擇檔案。")
    return list(file_paths)
#--

def create_ui():
    """創建主視窗 UI"""
    global root,progress,log_text,progress_label
    root = tk.Tk()
    root.title("檔案傳輸應用程式")
    root.geometry("640x480")  # 設定視窗大小


    # 創建日誌視窗
    log_text_label = tk.Label(root, text="日誌視窗")
    log_text_label.pack(pady=5)
    
    log_text = scrolledtext.ScrolledText(root, width=100, height=15, state='disabled')
    log_text.pack(pady=5)

    # 創建進度條
    progress_label = tk.Label(root, text="檔案傳輸進度")
    progress_label.pack(pady=5)
    
    progress = ttk.Progressbar(root, orient="horizontal", length=250, mode="determinate")
    progress.pack(pady=5)

    # 創建標籤
    label = tk.Label(root, text="選擇模式", font=("Arial", 15))
    label.pack(pady=10)


    # 創建伺服器和客戶端按鈕
    server_button = tk.Button(root, text="啟動伺服器", width=15, command=on_server_click)
    server_button.pack(pady=5)

    client_button = tk.Button(root, text="啟動客戶端", width=15, command=on_client_click)
    client_button.pack(pady=5)

 


    # 初始化日誌
    update_log("初始化視窗...")
    update_log("請選擇模式：啟動伺服器或啟動客戶端。")
    
    # 啟動主視窗
    root.after(1000, update_log)
    
    root.mainloop()








##def progress_thread(update_queue, file_size):
##    received_size = 0  # 初始化接收的檔案大小
##    while True:
##        try:
##            sent_size = update_queue.get(timeout=0.5)  # 從隊列中獲取當前傳送的大小
##            progress_value = (sent_size / file_size) * 100
##            progress["value"] = progress_value
##            root.update_idletasks()  # 更新 UI
##            if progress_value >= 100:
##                progress["value"] = 100  # 確保進度條顯示為 100
##                break  # 如果達到 100%，跳出循環
##        except queue.Empty:
##            if received_size == file_size:
##                progress["value"] = 100  # 如果文件大小相符，進度條設置為 100
##                break
##            break

        
            

##def mbps(update_queue, file_size):
##    last_update_time = time.time()  # 記錄開始時間
##    last_sent_size = 0
##    while True:
##       try:
##            sent_size = update_queue.get(timeout=0.5)  # 每次從隊列中獲取當前已發送的大小
##            current_time = time.time()
##            elapsed_time = current_time - last_update_time
##            
##            if elapsed_time > 0:
##                speed = (sent_size - last_sent_size) / 1_000_000 / elapsed_time
##            else:#                speed = 0.0
##               progress_label["text"] = f"檔案傳輸進度：{sent_size_mb:.2f} MB ({speed:.2f} MB/s)"
##               root.update_idletasks()
##               last_update_time = current_time
##               last_sent_size = sent_size
##       except queue.Empty:
##            if sent_size >= file_size:
##                progress_label["text"] = f"檔案傳輸完成，總大小：{file_size / 1_000_000:.2f} MB"
##                break

def mbps(update_queue, file_size):
    """
    使用 tqdm 和 tkinter 進行檔案傳輸進度顯示。
    
    :param update_queue: 傳送大小更新的隊列
    :param file_size: 總檔案大小（bytes）
    """
    last_update_time = time.time()  # 開始時間
    last_sent_size = 0  # 上一次更新的大小

    # 使用 tqdm 初始化進度條
    
    
    while True:
        try:
            sent_size = update_queue.get(timeout=0.5)  # 獲取當前已傳送的大小
            sent_size=sent_size
            current_time = time.time()
            elapsed_time = current_time - last_update_time
            
            # 計算速度 (MB/s)
            if elapsed_time > 0:
                speed = (sent_size - last_sent_size) / elapsed_time / 1_000_000
            else:
                speed = 0.0  # 避免除以 0
            
            # 更新 tkinter 進度條和速度顯示
            sent_size_mb = sent_size / 1_000_000
            progress_value = (sent_size / file_size) * 100
            progress["value"] = progress_value
            progress_label["text"] = f"檔案傳輸進度：{sent_size_mb:.2f} MB ({speed:.2f} MB/s)"
            root.update_idletasks()
            
            # 更新 tqdm 進度條

            
            # 記錄更新時間和大小
            last_update_time = current_time
            last_sent_size = sent_size
            if stop_flag==True:
                progress["value"] = 100
                progress_label["text"] = f"檔案傳輸完成，總大小：{file_size / 1_000_000:.2f} MB"
                
                break  # 檔案傳輸完成，跳出迴圈            
        except queue.Empty:
            if last_sent_size >= file_size:
                progress["value"] = 100
                progress_label["text"] = f"檔案傳輸完成，總大小：{file_size / 1_000_000:.2f} MB"
               
                break  # 檔案傳輸完成，跳出迴圈
            if stop_flag==True:
                progress["value"] = 100
                progress_label["text"] = f"檔案傳輸完成，總大小：{file_size / 1_000_000:.2f} MB"
                
                break  # 檔案傳輸完成，跳出迴圈


    
##def mbps(size, file_size):
##    """顯示傳輸速度"""
##    size=0
##    last_update_time = time.time()  # 記錄起始時間
##    last_sent_size = 0
##    
##    while not stop_flag:
##            try:
##                    size+=MB
##                    
##                    progress_value = (size / file_size) * 100
##                    progress["value"] = progress_value
##                    current_time = time.time()
##                    elapsed_time = current_time - last_update_time
##                    if received_size == file_size:
##                        progress["value"] = 100  # 如果文件大小相符，進度條設置為 100
##                        break
##                    if elapsed_time > 0:  # 確保計算不除以 0
##                        speed = (size ) / elapsed_time / 1_000_000  # 速度 MB/s
##                        progress_label["text"] = f"傳輸速度: {speed:.2f} MB/s"
##                        print(speed,size,file_size,elapsed_time)
##                        time.sleep(1)
##                        
##                    else:
##                        progress_label["text"] = "傳輸速度: 計算中..."
##
##                    last_update_time = current_time
##                    last_sent_size +=MB
##                    size=0
##                    
##                    if last_sent_size >= file_size:  # 如果已傳輸完成
##                     break
##            except queue.Empty:
##                break



            
def update_log(message=None):
    """更新日誌"""
    if message != None:
        log_text.config(state='normal')
        log_text.insert('end', message + '\n')
        log_text.yview('end')
        log_text.config(state='disabled')
        root.after(1000, update_log)

if __name__ == "__main__":
     
     BUFFER_SIZE =65536
     create_ui()
    





