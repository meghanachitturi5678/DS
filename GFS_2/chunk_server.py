import socket
import threading
import os
import sys
import pickle
import hashlib
import logging
import time

logging.basicConfig(filename='chunk_server.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

HEARTBEAT_INTERVAL = 5

class ChunkServer:
    def __init__(self, host, port, myChunkDir, filesystem, master_hosts_ports):
        self.filesystem = filesystem
        self.myChunkDir = myChunkDir
        self.host = host
        self.port = port
        self.chunkserver_info = []  # List of stored chunks
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        logging.info("Chunk Server initialized on host %s, port %d", host, port)
        self.master_hosts_ports = master_hosts_ports  # List of master servers

    def start(self):
        """Start the chunk server, begin listening and send periodic heartbeats."""
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
        self.listen()

    def listen(self):
        """Listen for incoming connections and handle each in a separate thread."""
        self.sock.listen(5)
        logging.info("Chunk Server started, listening on port %d", self.port)
        while True:
            client, address = self.sock.accept()
            client.settimeout(60)
            threading.Thread(target=self.handle_request, args=(client, address), daemon=True).start()

    def send_heartbeat(self):
        """Send periodic heartbeat messages to the MasterServer to indicate server activity."""
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            for host, port in self.master_hosts_ports:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((host, port))
                        heartbeat_message = {'command': 'heartbeat', 'port': self.port}
                        s.send(pickle.dumps(heartbeat_message))
                    logging.info("Heartbeat sent to MasterServer at %s:%d from port %d", host, port, self.port)
                    break  # If successful, no need to try other masters
                except Exception as e:
                    logging.error("Failed to send heartbeat to %s:%d: %s", host, port, e)
                    continue  # Try next master

    def calculate_checksum(self, data):
        """Calculate the checksum of data for integrity checks."""
        return hashlib.sha256(data).hexdigest()

    def handle_request(self, client, address):
        """Handle client and chunk server requests."""
        try:
            request = pickle.loads(client.recv(4096))
            command = request.get('command')

            if command in ['store', 'replicate']:
                filename = request['filename']
                chunk_id = request['chunk_id']
                data = request['data']
                checksum = request['checksum']
                response = self.store_chunk(chunk_id, filename, data, checksum)
                client.send(pickle.dumps(response))

            elif command == 'download':
                filename = request['filename']
                chunk_id = request['chunk_id']
                response = self.send_chunk(chunk_id, filename)
                client.send(pickle.dumps(response))

        except Exception as e:
            logging.error("Error handling request from %s: %s", address, e)
        finally:
            client.close()

    def store_chunk(self, chunk_id, filename, data, checksum):
        """Store chunk data from client, ensuring data integrity."""
        try:
            os.makedirs(self.myChunkDir, exist_ok=True)
            path = os.path.join(self.myChunkDir, f"{filename}_{chunk_id}")

            # Verify checksum
            if self.calculate_checksum(data) != checksum:
                logging.error("Checksum mismatch for chunk %s, possible data corruption.", chunk_id)
                return {'status': 'error', 'message': 'Checksum mismatch'}

            with open(path, 'wb') as f:
                f.write(data)
            logging.info("Stored chunk %s successfully.", chunk_id)

            self.chunkserver_info.append((filename, chunk_id))
            return {'status': 'success'}
        except Exception as e:
            logging.error("Failed to store chunk %s: %s", chunk_id, e)
            return {'status': 'error', 'message': str(e)}

    def send_chunk(self, chunk_id, filename):
        """Send the requested chunk to client, including checksum for verification."""
        try:
            path = os.path.join(self.myChunkDir, f"{filename}_{chunk_id}")
            with open(path, 'rb') as f:
                data = f.read()
                checksum = self.calculate_checksum(data)
                return {'status': 'success', 'data': data, 'checksum': checksum}
        except FileNotFoundError:
            logging.error("Requested chunk %s not found", chunk_id)
            return {'status': 'error', 'message': 'Chunk not found'}
        except Exception as e:
            logging.error("Failed to send chunk %s: %s", chunk_id, e)
            return {'status': 'error', 'message': str(e)}

if __name__ == "__main__":
    try:
        port_num = int(sys.argv[1])
        if len(sys.argv) < 3:
            print("Usage: python chunk_server.py <port> [master_host master_port] ...")
            sys.exit(1)

        master_hosts_ports = []
        if (len(sys.argv) - 2) % 2 != 0:
            print("Invalid number of arguments for masters.")
            sys.exit(1)
        for i in range(2, len(sys.argv), 2):
            master_host = sys.argv[i]
            master_port = int(sys.argv[i + 1])
            master_hosts_ports.append((master_host, master_port))

        filesystem = os.path.join(os.getcwd(), f"chunk_server_{port_num}")
        chunk_server = ChunkServer('localhost', port_num, filesystem, filesystem, master_hosts_ports)
        logging.info("Starting Chunk Server on port %d", port_num)
        chunk_server.start()
    except Exception as e:
        logging.critical("Failed to start chunk server: %s", e)
        sys.exit(1)
