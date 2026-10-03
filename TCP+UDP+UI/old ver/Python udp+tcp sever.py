import socket
import time
import tkinter as tk
from tkinter import filedialog
import hashlib
import os

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

def TCP_SERVER(HOST, PORT, file_paths):
    
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
                    
                    # 傳送檔案內容
                    with open(file_path, "rb") as f:
                        while chunk := f.read(1024):  # 每次讀取 1024 bytes
                            client_socket.sendall(chunk)
                    
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
    """計算檔案的 MD5 值"""
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(1024):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

if __name__ == "__main__":
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
    TCP_SERVER(ip, port,file_paths)
    
