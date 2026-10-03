import socket
import os
import hashlib
import time



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



def calculate_md5(file_path):
    """計算檔案的 MD5 值"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


if __name__ == "__main__":
    # 監聽與廣播端一致的埠號
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
##    ip = '127.0.0.1'
##    Port = 60000
    TCP_CLIENT(ip, Port, save_directory)

    
