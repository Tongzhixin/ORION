import subprocess
import time
import csv

def get_gpu_stats():
    # Run the nvidia-smi command and get the output
    result = subprocess.run(['nvidia-smi', '--query-gpu=memory.used,memory.total,utilization.gpu', '--format=csv,noheader,nounits'],
                            stdout=subprocess.PIPE, text=True)
    output = result.stdout.strip()
    
    # Parse the output
    lines = output.split('\n')
    gpu_stats = []
    for line in lines:
        memory_used, memory_total, utilization_gpu = map(int, line.split(','))
        gpu_stats.append({
            'memory_used': memory_used,
            'memory_total': memory_total,
            'utilization_gpu': utilization_gpu
        })
    return gpu_stats

def monitor_gpu_to_file(file_path, interval=1):
    try:
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            while True:
                stats = get_gpu_stats()
                row = []
                for gpu in stats:
                    row.extend([gpu['memory_used'], gpu['memory_total'], gpu['utilization_gpu']])
                writer.writerow(row)
                file.flush()  # Ensure data is written to the file
                time.sleep(interval)
    except KeyboardInterrupt:
        print("Monitoring stopped.")

# Specify the file path and start monitoring GPU with a 5-second interval
file_path = 'gpu_stats.csv'
monitor_gpu_to_file(file_path, 1)