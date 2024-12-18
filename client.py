import socket
import os
import pickle
import hashlib
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MASTER_SERVER_PORT = 7082
CHUNK_SIZE = 2048  # Consistent with the chunk size used in Master and ChunkServer

class Client:
    def __init__(self, master_host='localhost', master_port=MASTER_SERVER_PORT):
        self.master_host = master_host
        self.master_port = master_port

    def calculate_checksum(self, data):
        """Calculate the checksum of data for integrity checks."""
        return hashlib.sha256(data).hexdigest()

    def upload_file(self, filename):
        """Upload a file to the distributed file system."""
        if not os.path.isfile(filename):
            logging.error("File %s does not exist", filename)
            return

        file_size = os.path.getsize(filename)
        num_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE  # Calculate number of chunks
        logging.info("Uploading file %s, size %d bytes, %d chunks", filename, file_size, num_chunks)

        # Notify MasterServer about the upload
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_sock:
            master_sock.connect((self.master_host, self.master_port))
            upload_request = {'command': 'upload', 'filename': filename, 'file_size': file_size}
            master_sock.send(pickle.dumps(upload_request))
            response = pickle.loads(master_sock.recv(4096))

        if response.get('status') != 'success':
            logging.error("Failed to upload file: %s", response.get('message'))
            return

        chunk_allocation = response.get('chunks')
        # Send each chunk to its designated server
        with open(filename, 'rb') as f:
            for chunk_id, servers in chunk_allocation.items():
                data = f.read(CHUNK_SIZE)
                checksum = self.calculate_checksum(data)

                for server_port in servers:
                    self.send_chunk(server_port, filename, chunk_id, data, checksum)

    def send_chunk(self, server_port, filename, chunk_id, data, checksum):
        """Send a single chunk to a ChunkServer."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', server_port))
                chunk_request = {'command': 'store', 'filename': filename, 'chunk_id': chunk_id, 'data': data, 'checksum': checksum}
                s.send(pickle.dumps(chunk_request))
                response = pickle.loads(s.recv(4096))

                if response.get('status') == 'success':
                    logging.info("Successfully stored chunk %s on server %d", chunk_id, server_port)
                else:
                    logging.error("Failed to store chunk %s on server %d: %s", chunk_id, server_port, response.get('message'))
        except Exception as e:
            logging.error("Error sending chunk %s to server %d: %s", chunk_id, server_port, e)

    def download_file(self, filename):
        """Download a file from the distributed file system."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_sock:
            master_sock.connect((self.master_host, self.master_port))
            download_request = {'command': 'download', 'filename': filename}
            master_sock.send(pickle.dumps(download_request))
            response = pickle.loads(master_sock.recv(4096))

        if response.get('status') != 'success':
            logging.error("Failed to download file: %s", response.get('message'))
            return

        chunk_locations = response.get('chunk_locations')
        if not chunk_locations:
            logging.error("No chunks found for file %s", filename)
            return

        # Reconstruct the file by downloading each chunk
        with open(f"downloaded_{filename}", 'wb') as f:
            for chunk_id, servers in chunk_locations.items():
                data = self.retrieve_chunk(servers, filename, chunk_id)
                if data:
                    f.write(data)
                else:
                    logging.error("Failed to retrieve chunk %s for file %s", chunk_id, filename)
                    return

        logging.info("File %s downloaded successfully as downloaded_%s", filename, filename)

    def retrieve_chunk(self, servers, filename, chunk_id):
        """Retrieve a chunk from available servers and verify its checksum."""
        for server_port in servers:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(('localhost', server_port))
                    download_request = {'command': 'download', 'filename': filename, 'chunk_id': chunk_id}
                    s.send(pickle.dumps(download_request))
                    response = pickle.loads(s.recv(4096))

                if response.get('status') == 'success':
                    data = response['data']
                    checksum = response['checksum']
                    if self.calculate_checksum(data) == checksum:
                        logging.info("Successfully retrieved and verified chunk %s from server %d", chunk_id, server_port)
                        return data
                    else:
                        logging.warning("Checksum mismatch for chunk %s from server %d, trying next server", chunk_id, server_port)
            except Exception as e:
                logging.error("Failed to retrieve chunk %s from server %d: %s", chunk_id, server_port, e)

        return None

    def list_files(self):
        """Request a list of files from the MasterServer."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_sock:
            master_sock.connect((self.master_host, self.master_port))
            list_request = {'command': 'list_files'}
            master_sock.send(pickle.dumps(list_request))
            response = pickle.loads(master_sock.recv(4096))

        if isinstance(response, list):
            logging.info("Files available on the server:")
            for file in response:
                print(file)
        else:
            logging.error("Failed to retrieve file list: %s", response.get('message'))

    def lease_file(self, filename):
        """Request an exclusive lease on a file."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_sock:
            master_sock.connect((self.master_host, self.master_port))
            lease_request = {'command': 'lease', 'filename': filename}
            master_sock.send(pickle.dumps(lease_request))
            response = pickle.loads(master_sock.recv(4096))

        if response.get('status') == 'success':
            logging.info("Lease granted for file %s", filename)
        else:
            logging.warning("Lease request failed for file %s: %s", filename, response.get('message'))

    def unlease_file(self, filename):
        """Release the exclusive lease on a file."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_sock:
            master_sock.connect((self.master_host, self.master_port))
            unlease_request = {'command': 'unlease', 'filename': filename}
            master_sock.send(pickle.dumps(unlease_request))
            response = pickle.loads(master_sock.recv(4096))

        if response.get('status') == 'success':
            logging.info("Lease released for file %s", filename)
        else:
            logging.warning("Failed to release lease for file %s: %s", filename, response.get('message'))

    def run(self):
        """Run the client interaction loop."""
        while True:
            print("\nClient Menu:")
            print("1. Upload File")
            print("2. Download File")
            print("3. List Files")
            print("4. Lease File")
            print("5. Unlease File")
            print("6. Exit")

            choice = input("Enter your choice (1-6): ").strip()
            if choice == '1':
                filename = input("Enter the filename to upload: ").strip()
                self.upload_file(filename)
            elif choice == '2':
                filename = input("Enter the filename to download: ").strip()
                self.download_file(filename)
            elif choice == '3':
                self.list_files()
            elif choice == '4':
                filename = input("Enter the filename to lease: ").strip()
                self.lease_file(filename)
            elif choice == '5':
                filename = input("Enter the filename to unlease: ").strip()
                self.unlease_file(filename)
            elif choice == '6':
                print("Exiting client.")
                break
            else:
                print("Invalid choice. Please select a valid option.")

if __name__ == "__main__":
    client = Client()
    client.run()
