from typing import Any, Tuple
from dataclasses import dataclass, field
from queue import Queue
import threading
import socket
import json
import logging

logger = logging.getLogger("server_broadcasting_py")

Address = Tuple[str | int, str | int]


@dataclass
class Client:
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
    clients: list[Client] = field(default_factory=list)
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
            client = Client(
                client_socket=client_socket, data_queue=Queue(), address=address
            )
            self.clients.append(client)
            client.thread = threading.Thread(
                target=client.handler, args=(), daemon=True
            )
            client.thread.start()
