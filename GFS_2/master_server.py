import asyncio
import raftos
import socket
import threading
import pickle
import time
import logging
import math
import os

logging.basicConfig(filename='master_server.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

CHUNK_PORTS = [6467, 6468, 6469, 6470]
REPLICATION_FACTOR = 2
HEARTBEAT_INTERVAL = 5
LEASE_DURATION = 30  # Lease duration in seconds

class MasterStateMachine:
    def __init__(self):
        self.chunksize = 2048
        self.file_map = {}  # Maps filenames to their chunk information
        self.chunk_locations = {}  # Maps chunk IDs to their respective chunk servers
        self.leases = {}  # Tracks leases: {'filename': {'expires': <time>, 'client': <client_address>}}

    async def apply(self, command):
        """Apply committed log entries to the state machine."""
        cmd = command.get('cmd')
        if cmd == 'add_file':
            filename = command['filename']
            chunk_ids = command['chunk_ids']
            self.file_map[filename] = chunk_ids
        elif cmd == 'lease_file':
            filename = command['filename']
            lease_info = command['lease_info']
            self.leases[filename] = lease_info
        elif cmd == 'unlease_file':
            filename = command['filename']
            if filename in self.leases:
                del self.leases[filename]
        # Handle other commands as needed

class MasterServer:
    def __init__(self, host, port, peers):
        self.host = host
        self.port = port
        self.peers = peers  # List of peer master servers

        self.state_machine = MasterStateMachine()

        self.chunk_servers_info = {p: [] for p in CHUNK_PORTS}  # Tracks chunks held by each server
        self.active_servers = set()  # Set of active chunk servers for quick access

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        logging.info("Master Server initialized on host %s, port %d", host, port)

        # Raft setup
        self.node = raftos.Node(f'{self.host}:{self.port}', state_machine=self.state_machine)
        raftos.configure({
            'log_path': f'./logs/{self.port}',
            'quorum_size': (len(peers) + 1) // 2 + 1,
            'db_create': True,
        })

    async def start(self):
        """Start the master server and Raft node."""
        # Add peers
        for peer in self.peers:
            await raftos.add_node(f'{peer[0]}:{peer[1]}')

        asyncio.ensure_future(self.run_server())

        # Start Raft node
        await self.node.start()
        logging.info("Master Server Raft node started on %s:%d", self.host, self.port)

    async def run_server(self):
        """Run the TCP server to handle client requests."""
        self.sock.listen(5)
        logging.info("Master Server started, listening for connections.")

        threading.Thread(target=self.heartbeat, daemon=True).start()
        threading.Thread(target=self.check_replication_integrity, daemon=True).start()
        threading.Thread(target=self.lease_expiration_checker, daemon=True).start()

        loop = asyncio.get_event_loop()
        while True:
            client, address = await loop.run_in_executor(None, self.sock.accept)
            threading.Thread(target=self.handle_client, args=(client, address), daemon=True).start()

    def num_chunks(self, size):
        return math.ceil(size / self.state_machine.chunksize)

    def is_leader(self):
        leader = raftos.get_leader()
        return leader == f'{self.host}:{self.port}'

    def get_leader_address(self):
        leader = raftos.get_leader()
        if leader:
            leader_host, leader_port = leader.split(':')
            return leader_host, int(leader_port)
        else:
            return None, None

    def handle_client(self, client, address):
        """Handle incoming client requests."""
        try:
            request = pickle.loads(client.recv(4096))
            command = request.get('command')

            if not self.is_leader():
                # Redirect client to leader
                leader_host, leader_port = self.get_leader_address()
                if leader_host and leader_port:
                    response = {'status': 'redirect', 'leader_host': leader_host, 'leader_port': leader_port}
                else:
                    response = {'status': 'error', 'message': 'No leader elected yet'}
                client.send(pickle.dumps(response))
                client.close()
                return

            if command == 'upload':
                filename = request['filename']
                file_size = request['file_size']
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                response = loop.run_until_complete(self.handle_upload(filename, file_size))
                client.send(pickle.dumps(response))
                loop.close()

            elif command == 'download':
                filename = request['filename']
                response = self.get_chunk_locations(filename)
                client.send(pickle.dumps(response))

            elif command == 'list_files':
                response = list(self.state_machine.file_map.keys())
                client.send(pickle.dumps(response))

            elif command == 'lease':
                filename = request['filename']
                client_address = address
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                response = loop.run_until_complete(self.lease_file(filename, client_address))
                client.send(pickle.dumps(response))
                loop.close()

            elif command == 'unlease':
                filename = request['filename']
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                response = loop.run_until_complete(self.unlease_file(filename))
                client.send(pickle.dumps(response))
                loop.close()

            elif command == 'heartbeat':
                port = request['port']
                self.update_server_status(port)
                # No response needed

            client.close()
        except Exception as e:
            logging.error("Error handling client request from %s: %s", address, e)
            client.close()

    async def handle_upload(self, filename, file_size):
        """Handle file upload requests by allocating chunks and assigning servers."""
        if filename in self.state_machine.file_map:
            return {'status': 'error', 'message': 'File already exists'}

        num_chunks = self.num_chunks(file_size)
        chunk_ids = [f"{filename}_chunk_{i}" for i in range(num_chunks)]
        # Update state via Raft log
        await raftos.commit({'cmd': 'add_file', 'filename': filename, 'chunk_ids': chunk_ids})

        # Allocate chunks to servers
        chunk_allocation = self.allocate_chunks(chunk_ids)
        return {'status': 'success', 'chunks': chunk_allocation}

    def get_chunk_locations(self, filename):
        """Return chunk locations for a requested file."""
        if filename not in self.state_machine.file_map:
            return {'status': 'error', 'message': 'File not found'}

        chunk_locations = {chunk_id: self.state_machine.chunk_locations.get(chunk_id, []) for chunk_id in self.state_machine.file_map[filename]}
        return {'status': 'success', 'chunk_locations': chunk_locations}

    async def lease_file(self, filename, client_address):
        """Lease a file to a client for exclusive write access."""
        current_time = time.time()
        if filename in self.state_machine.leases and self.state_machine.leases[filename]['expires'] > current_time:
            return {'status': 'error', 'message': f'File {filename} is already leased.'}

        # Grant lease and set expiration time
        lease_info = {
            'expires': current_time + LEASE_DURATION,
            'client': client_address
        }
        await raftos.commit({'cmd': 'lease_file', 'filename': filename, 'lease_info': lease_info})
        logging.info("Leased file %s to client %s for %d seconds", filename, client_address, LEASE_DURATION)
        return {'status': 'success', 'message': f'File {filename} leased for {LEASE_DURATION} seconds.'}

    async def unlease_file(self, filename):
        """Release a lease on a file, allowing other clients to access it."""
        if filename in self.state_machine.leases:
            await raftos.commit({'cmd': 'unlease_file', 'filename': filename})
            logging.info("Unleased file %s", filename)
            return {'status': 'success', 'message': f'File {filename} has been unleased.'}
        else:
            return {'status': 'error', 'message': f'File {filename} was not leased.'}

    def lease_expiration_checker(self):
        """Periodically check and expire leases that have timed out."""
        while True:
            time.sleep(5)  # Check every 5 seconds
            current_time = time.time()
            expired_leases = [file for file, lease in self.state_machine.leases.items() if lease['expires'] < current_time]
            for filename in expired_leases:
                # Since this modifies state, it needs to be committed via Raft
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(raftos.commit({'cmd': 'unlease_file', 'filename': filename}))
                loop.close()
                logging.info("Lease expired for file %s", filename)

    def allocate_chunks(self, chunk_ids):
        """Allocate chunks across available chunk servers with replication."""
        chunk_allocation = {}
        for chunk_id in chunk_ids:
            servers = self.select_chunk_servers(REPLICATION_FACTOR)
            if not servers:
                logging.error("No active chunk servers available to allocate chunk %s.", chunk_id)
                continue
            self.state_machine.chunk_locations[chunk_id] = servers

            # Track chunk assignments for each server
            for server in servers:
                self.chunk_servers_info[server].append(chunk_id)

            chunk_allocation[chunk_id] = servers
        return chunk_allocation

    def select_chunk_servers(self, replication_factor):
        """Select servers for chunk replication based on their current load and active status."""
        active_servers_info = {s: self.chunk_servers_info[s] for s in self.active_servers}
        available_servers = sorted(active_servers_info.items(), key=lambda x: len(x[1]))
        selected_servers = [server for server, _ in available_servers[:replication_factor]]

        if len(selected_servers) < replication_factor:
            logging.warning("Not enough active servers for full replication.")

        return selected_servers

    def update_server_status(self, port):
        """Update active server list based on heartbeat signals."""
        if port not in self.active_servers:
            self.active_servers.add(port)
            logging.info("Server on port %d is now active", port)

    def heartbeat(self):
        """Check active status of all chunk servers periodically."""
        logging.info("Heartbeat check initiated.")
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            inactive_servers = set(CHUNK_PORTS) - self.active_servers
            for port in inactive_servers:
                self.handle_server_failure(port)
            self.active_servers.clear()  # Reset active status for next interval

    def handle_server_failure(self, port):
        """Handle chunk server failure by reallocating chunks."""
        logging.warning("Chunk server on port %d has failed", port)
        if port in self.chunk_servers_info:
            for chunk_id in self.chunk_servers_info[port]:
                # Reallocate the failed chunk to another active server
                self.reallocate_chunk(chunk_id, port)

            # Clear failed server's data
            self.chunk_servers_info[port] = []

    def reallocate_chunk(self, chunk_id, failed_server):
        """Reallocate chunk replicas when a server goes down."""
        # Remove the failed server from chunk locations
        if chunk_id in self.state_machine.chunk_locations:
            if failed_server is not None:
                self.state_machine.chunk_locations[chunk_id] = [s for s in self.state_machine.chunk_locations[chunk_id] if s != failed_server]

            # Add a new replica if replication factor is not met
            if len(self.state_machine.chunk_locations[chunk_id]) < REPLICATION_FACTOR:
                new_servers = self.select_chunk_servers(1)
                if new_servers:
                    new_server = new_servers[0]
                    self.state_machine.chunk_locations[chunk_id].append(new_server)
                    self.chunk_servers_info[new_server].append(chunk_id)
                    logging.info("Reallocated chunk %s to server on port %d", chunk_id, new_server)
                else:
                    logging.warning("No available servers to reallocate chunk %s", chunk_id)

    def check_replication_integrity(self):
        """Periodically verify that each chunk has the correct replication level."""
        while True:
            time.sleep(HEARTBEAT_INTERVAL * 3)
            for chunk_id, servers in self.state_machine.chunk_locations.items():
                if len(servers) < REPLICATION_FACTOR:
                    logging.warning("Chunk %s under-replicated, current replicas: %s", chunk_id, servers)
                    self.reallocate_chunk(chunk_id, None)

if __name__ == "__main__":
    import sys
    import asyncio

    if len(sys.argv) < 2:
        print("Usage: python master_server.py <port> [peer1_host peer1_port] [peer2_host peer2_port] ...")
        sys.exit(1)

    host = 'localhost'
    port = int(sys.argv[1])
    peers = []

    if len(sys.argv) > 2:
        if (len(sys.argv) - 2) % 2 != 0:
            print("Invalid number of arguments for peers.")
            sys.exit(1)
        for i in range(2, len(sys.argv), 2):
            peer_host = sys.argv[i]
            peer_port = int(sys.argv[i + 1])
            peers.append((peer_host, peer_port))

    master = MasterServer(host, port, peers)
    logging.info("Master Server Running on port %d", port)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(master.start())
    loop.run_forever()
