from typing import Any, Tuple, Callable
from dataclasses import dataclass, field
from queue import Queue
import threading
import socket
import json
import logging

logger = logging.getLogger("server_broadcasting_py")

Address = Tuple[str | int, str | int]


@dataclass
class ServerClient:
    client_socket: socket.socket
    data_queue: Queue[dict[str, Any]]
    address: Address
    connected: bool = True
    thread: threading.Thread | None = None

    def handler(self):
        while True:
            caption = self.data_queue.get()
            try:
                self.client_socket.send(json.dumps(caption).encode())
            except ConnectionError as e:
                self.connected = False
                logger.info(f"Client disconnected {self} {str(e)}")
                return
            finally:
                self.data_queue.task_done()


@dataclass
class Server:
    socket_address: Address
    clients: list[ServerClient] = field(default_factory=list)
    accept_connections_thread: threading.Thread | None = None

    def start(self):
        return self.start_accepting_connections()

    def start_accepting_connections(self, thread_args=dict(daemon=True)):
        self.accept_connections_thread = threading.Thread(
            target=self.accept_connections, **thread_args
        )
        self.accept_connections_thread.start()
        return self.accept_connections_thread

    def broadcast(self, data_to_broadcast):
        self.disconnected_indices = []

        for i, client in enumerate(self.clients):
            if not client.connected:
                self.disconnected_indices.append(i)
                continue
            client.data_queue.put(data_to_broadcast)

        self.clients_remove_disconnected()

    def clients_remove_disconnected(self):
        for index in reversed(self.disconnected_indices):
            logger.info(
                f"client[{index}] disconnected and removed {self.clients[index]}"
            )
            del self.clients[index]
        self.disconnected_indices = []

    def accept_connections(
        self,
        socket_kwargs=dict(family=socket.AF_INET, type=socket.SOCK_STREAM),
        listen_kwargs=dict(),
    ):
        """
        appends Clients to `server.clients` as connections are received
        """
        server_socket = socket.socket(**socket_kwargs)
        server_socket.bind(self.socket_address)
        server_socket.listen(**listen_kwargs)

        while True:
            logger.info(f"Listening on {self.socket_address}")
            client_socket, address = server_socket.accept()
            logger.info(f"New connection from {address}")
            client = ServerClient(
                client_socket=client_socket, data_queue=Queue(), address=address
            )
            self.clients.append(client)
            client.thread = threading.Thread(
                target=client.handler, args=(), daemon=True
            )
            client.thread.start()


class Client:
    def __init__(self, server_address: tuple[str, int], callback: Callable[[dict], Any]):
        self.server_address = server_address
        self.callback = callback
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connected = False
        self.receive_thread: threading.Thread | None = None
        self.buffer = ''

    def connect(self) -> None:
        """Establishes a connection to the server and starts the receiver thread."""
        try:
            self.socket.connect(self.server_address)
            self.connected = True
            self.start_receiver()
            logger.info(f"Connected to server at {self.server_address}")
        except Exception as e:
            logger.error(f"Failed to connect to server: {e}")
            raise

    def start_receiver(self) -> None:
        """Starts the receiver thread to listen for incoming messages from the server."""
        self.receive_thread = threading.Thread(target=self.receive_loop, daemon=True)
        self.receive_thread.start()

    def receive_loop(self) -> None:
        """Continuously receives data from the server, processes JSON messages, and triggers the callback."""
        decoder = json.JSONDecoder()
        while self.connected:
            try:
                data = self.socket.recv(4096*4)  # Read up to 4096 bytes
                if not data:
                    logger.info("Server closed the connection.")
                    break
                self.buffer += data.decode('utf-8')
                # Process all complete JSON messages in the buffer
                while self.buffer:
                    try:
                        obj, idx = decoder.raw_decode(self.buffer)
                        self.callback(obj)
                        self.buffer = self.buffer[idx:].lstrip()
                    except json.JSONDecodeError:
                        # Incomplete JSON data, wait for more data
                        break
            except (ConnectionError, OSError) as e:
                logger.error(f"Connection error: {e}")
                break
        self.connected = False
        self.socket.close()
        logger.info("Disconnected from server.")

    def close(self) -> None:
        """Closes the connection and stops the receiver thread."""
        self.connected = False
        if self.socket:
            try:
                self.socket.close()
            except Exception as e:
                logger.error(f"Error closing socket: {e}")
        if self.receive_thread:
            self.receive_thread.join()
