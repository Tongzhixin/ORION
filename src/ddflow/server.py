import socket
import threading
import pyarrow as pa

def handle_client(client_socket):
    while True:
        buffer = bytearray()
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            buffer.extend(chunk)
            if len(chunk) < 4096:
                break

        if buffer:
            source = pa.BufferReader(buffer)
            reader = pa.RecordBatchStreamReader(source)
            table = reader.read_all()
            print(f"Received table: {table}")
    client_socket.close()

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('0.0.0.0', 12345))
    server.listen(5)
    print("Server listening on port 12345")

    while True:
        client_socket, addr = server.accept()
        print(f"Accepted connection from {addr}")
        client_handler = threading.Thread(target=handle_client, args=(client_socket,))
        client_handler.start()

if __name__ == "__main__":
    start_server()