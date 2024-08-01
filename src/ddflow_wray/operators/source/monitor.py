import psutil
import time
import datetime

def get_system_info(interval=1):
    # 获取初始网络通信量
    net_io_start = psutil.net_io_counters()
    
    # 获取所有进程的RSS内存之和
    # rss_memory_total = sum(proc.memory_info().rss for proc in psutil.process_iter())
        # time_start=time.time()
    processes = psutil.process_iter(['pid', 'name', 'memory_info'])
    rss_memory_total = sum(proc.info['memory_info'].rss for proc in processes) / (1024 ** 2)  # Convert to GB

    # 等待1秒钟
    time.sleep(interval)
    
    # 获取1秒钟后的网络通信量
    net_io_end = psutil.net_io_counters()
    
    # 计算网络收发速率（以MB为单位）
    sent_rate = (net_io_end.bytes_sent - net_io_start.bytes_sent) / (1024 * 1024)
    recv_rate = (net_io_end.bytes_recv - net_io_start.bytes_recv) / (1024 * 1024)
    
    # 获取CPU利用率之和
    cpu_usage = psutil.cpu_percent()
    
    # 获取内存占用信息（RSS内存之和）
    memory_usage = rss_memory_total
    
    # 获取网络通信量（以MB为单位）
    bytes_sent = net_io_end.bytes_sent / (1024 * 1024)
    bytes_recv = net_io_end.bytes_recv / (1024 * 1024)
    
    return cpu_usage, memory_usage, bytes_sent, bytes_recv, sent_rate, recv_rate

def main():
    output_file = "system_info.log"
    with open(output_file, "a") as f:
        # 写入标题
        title = "Timestamp, CPU Usage (%), Memory Usage (MB), Bytes Sent (MB), Bytes Received (MB), Sent Rate (MB/s), Receive Rate (MB/s)\n"
        f.write(title)
        
        while True:
            cpu_usage, memory_usage, bytes_sent, bytes_recv, sent_rate, recv_rate = get_system_info()
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_entry = f"{timestamp},{cpu_usage},{memory_usage:.2f},{bytes_sent:.2f},{bytes_recv:.2f},{sent_rate:.2f},{recv_rate:.2f}\n"
            f.write(log_entry)
            f.flush()

if __name__ == "__main__":
    main()