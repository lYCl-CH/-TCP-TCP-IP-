import socket
import time
import tkinter as tk
from tkinter import filedialog, messagebox,ttk  
import hashlib
import os

#-------
def udp_receive_ip_and_port(port, buffer_size=1024):
    """
    接收區域網路的 UDP 廣播訊息，解析 IP 和埠號。

    :param port: 要監聽的埠號
    :param buffer_size: 接收資料的緩衝大小 (預設為 1024 bytes)
    """
    # 建立 UDP Socket
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # 設置 Socket 為可重複使用
    udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # 綁定至本地所有網路介面及指定的埠
    udp_socket.bind(("", port))
    
    print(f"正在監聽 UDP 廣播訊息 (埠: {port})...")
    try:
        while True:
            # 接收來自廣播的訊息
            message, address = udp_socket.recvfrom(buffer_size)
            decoded_message = message.decode('utf-8')
            print(f"接收到訊息: {decoded_message} 來自: {address}")
            
            # 回傳自己的 IP 給發送端
            response_message = f"My IP is: {socket.gethostbyname(socket.gethostname())}"
            udp_socket.sendto(response_message.encode('utf-8'), address)
            print(f"回傳訊息: {response_message} 來自: {socket.gethostname()}")
            return  decoded_message
            break
    except KeyboardInterrupt:
        print("停止接收廣播訊息")
    finally:
        udp_socket.close()


def TCP_CLIENT(HOST, PORT, save_directory):
    client_socket = None
    retry_count = 5  # 最大重試次數
    retry_interval = 3  # 每次重試的間隔（秒）
    
    for attempt in range(1, retry_count + 1):
        try:
            # 嘗試建立連接
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((HOST, PORT))  # 嘗試連接伺服器
            print(f"已連接到伺服器 {HOST}:{PORT}")
            break  # 成功連接後跳出循環

        except ConnectionRefusedError:
            print(f"無法連接到伺服器 {HOST}:{PORT} (重試次數: {attempt}/{retry_count})，請確認伺服器是否啟動並且可用。")
            if attempt < retry_count:
                print(f"等待 {retry_interval} 秒後重試...")
                time.sleep(retry_interval)
            else:
                print("已達最大重試次數，連接失敗。")
                return  # 超過最大重試次數，退出程式

        except Exception as e:
            print(f"發生錯誤: {e}")
            return

    try:
        while True:
            # 接收檔案的元數據（檔案名稱、大小、MD5）
            metadata = b""
            
            while not metadata.endswith(b"\n"):  # 確保接收到完整的元數據
                metadata += client_socket.recv(1024)
            metadata = metadata.decode('utf-8').strip()

            if metadata == "END_OF_TRANSMISSION":  # 檢查結束訊息
                print("所有檔案接收完成。")
                break  # 結束循環

            if "|" not in metadata:
                print(f"無效的 metadata：{metadata}")
                continue  # 忽略不正確的 metadata

            file_name, file_size, file_md5 = metadata.split('|')
            file_size = int(file_size)
            save_path = os.path.join(save_directory, file_name)
            print(f"準備接收檔案：{file_name}，大小：{file_size} bytes，MD5：{file_md5}")

            # 接收檔案數據
            received_size = 0
            with open(save_path, "wb") as f:
                while received_size < file_size:
                    data = client_socket.recv(1024)
                    if b"END_OF_FILE" in data:
                        print(f"接收到 END_OF_FILE: {data}")
                        data = data.replace(b"END_OF_FILE", b"")  # 移除 END_OF_FILE
                        f.write(data)
                        break
                    f.write(data)
                    received_size += len(data)

            # 驗證 MD5
            received_md5 = calculate_md5-(save_path)
            if received_md5 == file_md5:
                print(f"檔案 '{file_name}' 接收成功且 MD5 驗證通過。")
                client_socket.sendall(b"ACK")
            else:
                print(f"檔案 '{file_name}' 接收失敗或損毀，MD5 驗證不匹配。")
                client_socket.sendall(b"RESEND")
            
            # 檢查是否是最後一個檔案，並發送 END_OF_TRANSMISSION
            # 如果所有檔案都已經接收完畢，則返回結束訊息
            data = client_socket.recv(1024)
            if b"END_OF_TRANSMISSION" in data:
                print("所有檔案接收完成。")
                break  # 結束循環

    except Exception as e:
            print(f"發生錯誤: {e}")
    finally:
            if client_socket:
                client_socket.close()
            print("連接已關閉。")





#------
def udp_broadcast_ip_and_port(ip, port, interval=3):
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
            print(f"已廣播: {message}")
            
            # 等待指定的間隔時間
            time.sleep(interval)
            
            # 接收回應的訊息 (自己的IP)
            udp_socket.settimeout(1)  # 設置超時
            try:
                response, address = udp_socket.recvfrom(1024)
                #response_message =
                print(f"接收到回應: {response.decode('utf-8')} 來自: {address}")
                break  # 收到回應後停止廣播
            except socket.timeout:
                continue  # 如果沒有收到回應，繼續廣播
    except KeyboardInterrupt:
        print("廣播停止")
    finally:
        udp_socket.close()

def TCP_SERVER(HOST, PORT, file_paths, progress):
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen()  # 開始監聽
        print(f"伺服器正在 {HOST}:{PORT} 等待客戶端連接...")
        
        while True:  # 持續等待新的客戶端
            client_socket, client_address = server_socket.accept()
            print(f"客戶端 {client_address} 已連接。")
            
            try:
                for file_path in file_paths:
                    # 傳送檔案名稱、大小和 MD5
                    file_name = os.path.basename(file_path)
                    file_size = os.path.getsize(file_path)
                    file_md5 = calculate_md5(file_path)
                    metadata = f"{file_name}|{file_size}|{file_md5}\n"
                    client_socket.sendall(metadata.encode('utf-8'))

                    # 初始化進度條
                    sent_size = 0
                    update_progress(progress, 0)  # 重置進度條
                    
                    # 傳送檔案內容
                    with open(file_path, "rb") as f:
                        while chunk := f.read(1024):  # 每次讀取 1024 bytes
                            client_socket.sendall(chunk)
                            sent_size += len(chunk)
                            progress_value = (sent_size / file_size) * 100
                            update_progress(progress, progress_value)  # 更新進度條
                            
                    print(f"檔案 '{file_name}' 已成功傳送，等待客戶端確認...")
                    client_socket.sendall(b"END_OF_FILE\n")  # 檔案結束信號
                    
                    # 等待客戶端的回應
                    server_response = client_socket.recv(1024).decode('utf-8').strip()
                    if server_response == "ACK":
                        print(f"客戶端確認檔案 '{file_name}' 接收成功。")
                    elif server_response == "RESEND":
                        print(f"客戶端要求重傳檔案 '{file_name}'，正在重傳...")
                        continue  # 重新傳送該檔案
                    else:
                        print(f"未知的回應：{server_response}")
                        break
                
                # 所有檔案傳送完畢，發送 END_OF_TRANSMISSION 訊息
                client_socket.sendall(b"END_OF_TRANSMISSION\n")
                print("所有檔案已傳送完成。")
                break  # 跳出伺服器的處理循環
            
            except Exception as e:
                print(f"傳送檔案時發生錯誤：{e}")
            
            finally:

                client_socket.close()
                print(f"與客戶端 {client_address} 的連線已關閉。")


def select_files():
    """彈出檔案選擇對話框，讓使用者選擇多個檔案"""
    root = tk.Tk()
    root.withdraw()  # 隱藏主視窗
    file_paths = filedialog.askopenfilenames(
        title="選擇要傳送的檔案",
        filetypes=[("All files", "*.*")]
    )
    if not file_paths:
        print("未選擇檔案，程式結束。")
        exit()
    return list(file_paths)
def calculate_md5(file_path):
    if i==1:
        """計算檔案的 MD5 值"""
        md5_hash = hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk := f.read(1024):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    if i==2:
        """計算檔案的 MD5 值"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()




def TCP_CLIENT(HOST, PORT, save_directory):
    client_socket = None
    retry_count = 5  # 最大重試次數
    retry_interval = 3  # 每次重試的間隔（秒）
    
    for attempt in range(1, retry_count + 1):
        try:
            # 嘗試建立連接
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((HOST, PORT))  # 嘗試連接伺服器
            print(f"已連接到伺服器 {HOST}:{PORT}")
            break  # 成功連接後跳出循環

        except ConnectionRefusedError:
            print(f"無法連接到伺服器 {HOST}:{PORT} (重試次數: {attempt}/{retry_count})，請確認伺服器是否啟動並且可用。")
            if attempt < retry_count:
                print(f"等待 {retry_interval} 秒後重試...")
                time.sleep(retry_interval)
            else:
                print("已達最大重試次數，連接失敗。")
                return  # 超過最大重試次數，退出程式

        except Exception as e:
            print(f"發生錯誤: {e}")
            return

    try:
        while True:
            # 接收檔案的元數據（檔案名稱、大小、MD5）
            metadata = b""
            
            while not metadata.endswith(b"\n"):  # 確保接收到完整的元數據
                metadata += client_socket.recv(1024)
            metadata = metadata.decode('utf-8').strip()

            if metadata == "END_OF_TRANSMISSION":  # 檢查結束訊息
                print("所有檔案接收完成。")
                break  # 結束循環

            if "|" not in metadata:
                print(f"無效的 metadata：{metadata}")
                continue  # 忽略不正確的 metadata

            file_name, file_size, file_md5 = metadata.split('|')
            file_size = int(file_size)
            save_path = os.path.join(save_directory, file_name)
            print(f"準備接收檔案：{file_name}，大小：{file_size} bytes，MD5：{file_md5}")

            # 接收檔案數據
            received_size = 0
            with open(save_path, "wb") as f:
                while received_size < file_size:
                    data = client_socket.recv(1024)
                    if b"END_OF_FILE" in data:
                        print(f"接收到 END_OF_FILE: {data}")
                        data = data.replace(b"END_OF_FILE", b"")  # 移除 END_OF_FILE
                        f.write(data)
                        break
                    f.write(data)
                    received_size += len(data)

                    # 更新進度條
                    update_progress(progress, (received_size / file_size) * 100)

            # 驗證 MD5
            received_md5 = calculate_md5(save_path)
            if received_md5 == file_md5:
                print(f"檔案 '{file_name}' 接收成功且 MD5 驗證通過。")
                client_socket.sendall(b"ACK")
            else:
                print(f"檔案 '{file_name}' 接收失敗或損毀，MD5 驗證不匹配。")
                client_socket.sendall(b"RESEND")
            
            # 檢查是否是最後一個檔案，並發送 END_OF_TRANSMISSION
            # 如果所有檔案都已經接收完畢，則返回結束訊息
            data = client_socket.recv(1024)
            if b"END_OF_TRANSMISSION" in data:
                print("所有檔案接收完成。")
                break  # 結束循環

    except Exception as e:
            print(f"發生錯誤: {e}")
    finally:
            if client_socket:
                client_socket.close()
            print("連接已關閉。")
##-----
def on_client_click():
    """處理客戶端的動作"""
    listening_port = 60000
    ip_info = udp_receive_ip_and_port(listening_port)
    if "IP: " in ip_info:
        ip = ip_info.split("IP: ")[1].split(",")[0].strip()
        port_str = ip_info.split("Port: ")[1].strip()
        port = int(port_str)
        print(f"IP: {ip}, 埠號: {port}")
        
        save_directory = "./received_files"
        if not os.path.exists(save_directory):
            os.makedirs(save_directory)
        TCP_CLIENT(ip, port, save_directory)
    else:
        messagebox.showerror("錯誤", "無法取得 IP 或埠號資訊。")
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
    global root,progress
    root = tk.Tk()
    root.title("檔案傳輸應用程式")


    # 設定視窗大小
    root.geometry("400x250")


    # 創建標籤
    label = tk.Label(root, text="選擇模式", font=("Arial", 14))
    label.pack(pady=20)


    # 創建伺服器和客戶端按鈕
    server_button = tk.Button(root, text="啟動伺服器", width=20, command=on_server_click)
    server_button.pack(pady=10)


    client_button = tk.Button(root, text="啟動客戶端", width=20, command=on_client_click)
    client_button.pack(pady=10)

    
    # 創建進度條
    progress_label = tk.Label(root, text="檔案傳輸進度")
    progress_label.pack(pady=10)


    progress = ttk.Progressbar(root, orient="horizontal", length=250, mode="determinate")
    progress.pack(pady=10)
    # 啟動應用程式
    root.mainloop()
def on_server_click():
            global i
            i=1
            file_paths = select_files()
            print(f"選擇的檔案：{file_paths}")
            
            # 獲取本機 IP
            ip = socket.gethostbyname(socket.gethostname())
            HOST = ip
            # 設定埠號
            port = 60000  # 可根據需求調整埠號
            # 每三秒廣播一次 IP 和埠號
            udp_broadcast_ip_and_port(ip, port, interval=3)
            port = 60000
            TCP_SERVER(ip, port,file_paths,progress)


def on_client_click():
        global i
        i=2
        listening_port = 60000  # 必須與廣播端的埠號相同
        ip_info=udp_receive_ip_and_port(listening_port)
        print(f"接收到的訊息: {ip_info}")
        if "IP: " in ip_info:
            ip = ip_info.split("IP: ")[1].split(",")[0].strip()
            port_str = ip_info.split("Port: ")[1].strip()
            Port = int(port_str)  # 確保 Port 是整數
            print(f"提取的 IP: {ip}, 埠號: {Port}")
        else:
            print("訊息中未包含 IP 或埠號")
            exit(1)  # 若無法提取 IP 或埠號，則結束程式
        # 如果目錄不存在，則建立
        save_directory = "./received_files"
        if not os.path.exists(save_directory):
            os.makedirs(save_directory)
        TCP_CLIENT(ip, Port, save_directory)    

def update_progress(progress, value):
    """更新進度條的顯示"""
    progress['value'] = value
    root.update_idletasks()  # 刷新進度條顯示

    

if __name__ == "__main__":
     i=0
     create_ui()
    
#     i=int(input('1.sever \n2.client\n'))




