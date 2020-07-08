import datetime
import json
import socket
import time
import logging
import threading
from contextlib import contextmanager
from typing import Tuple, Dict, Any, Union, List, Iterator, Optional


class Transaction:
    def __init__(self):
        self._operations: List[bytea] = []

    def push(
        self,
        task_name: str,
        payload: Any = None,
        *,
        queue: str = None,
        deadqueue: str = None,
        retry: int = None,
        execute_at: datetime.datetime = None,
    ) -> None:
        """
        Publish a task.
        """
        request: Dict[str, Any] = {"name": task_name}
        if queue:
            request["queue"] = queue
        if deadqueue:
            request["deadqueue"] = deadqueue
        if payload:
            request["payload"] = payload
        if retry is not None:
            request["retry"] = retry
        if execute_at is not None:
            request["execute_at"] = execute_at.isoformat()

        raw = json.dumps(request)
        self._operations.append(f"PUSH {raw}\n".encode("utf8"))

    def ack(self, task_id: int) -> None:
        """
        Acknowledge the task with given ID. Task must be first returned to this
        client as a result of fetch call.
        Once acknowledged, task is forever deleted from the queue.
        """
        raw = json.dumps({"id": task_id})
        self._operations.append(f"ACK {raw}\n".encode("utf8"))


class Client:
    _heartbeat_sec: float = 2

    def __init__(self):
        self._lock = threading.Lock()
        self._client = BareClient()
        self._last_request: int = int(time.time())
        self._heartbeat: threading.Thread = None

    def _update_last_request_time(self) -> None:
        self._last_request = int(time.time())

    def is_connected(self) -> bool:
        return self._client.is_connected()

    def connect(self, host: str, port: int) -> None:
        """
        """
        with self._lock:
            self._client.connect(host, port)
            # A heartbeat ping activity is maintained in the background.
            if not self._heartbeat:
                self._heartbeat = threading.Thread(
                    target=_heartbeat_loop, args=(self,),
                )
                self._heartbeat.start()

    def disconnect(self) -> None:
        """
        """
        with self._lock:
            self._client.disconnect()
            self._heartbeat.join()

    def quit(self) -> None:
        """
        """
        with self._lock:
            self._client.quit()
            self._update_last_request_time()

    def ping(self) -> None:
        """
        """
        with self._lock:
            self._client.ping()
            self._update_last_request_time()

    def push(
        self,
        task_name: str,
        payload: Any = None,
        *,
        queue: str = None,
        deadqueue: str = None,
        retry: int = None,
        execute_at: datetime.datetime = None,
    ) -> int:
        """
        """
        with self._lock:
            res = self._client.push(
                task_name=task_name,
                payload=payload,
                queue=queue,
                deadqueue=deadqueue,
                retry=retry,
                execute_at=execute_at,
            )
            self._update_last_request_time()
            return res

    def fetch(
        self,
        queues: Union[Tuple[str], List[str]] = None,
        *,
        timeout: Union[int, str] = None,
    ) -> Any:
        """
        """
        with self._lock:
            res = self._client.fetch(queues=queues, timeout=timeout)
            self._update_last_request_time()
            return res

    def ack(self, task_id: int) -> None:
        """
        """
        with self._lock:
            self._client.ack(task_id=task_id)
            self._update_last_request_time()

    def nack(self, task_id: int) -> None:
        """
        """
        with self._lock:
            self._client.nack(task_id=task_id)
            self._update_last_request_time()

    @contextmanager
    def atomic(self) -> Iterator[Transaction]:
        """
        """
        with self._lock:
            with self._client.atomic() as tx:
                yield tx
            self._update_last_request_time()


def _heartbeat_loop(c: Client) -> None:
    while True:
        time.sleep(c._heartbeat_sec)

        with c._lock:
            if not c._client.is_connected():
                return
            try:
                c._client.ping()
            except Exception:
                c._client.disconnect()
                return
            c._last_request = int(time.time())


class BareClient:
    def __init__(self):
        self._sock: Optional[_LoggedSocket] = None
        self._log = logging.getLogger("masenko.client")

    def is_connected(self) -> bool:
        """
        Returns True if the connection this client is connected to the server.
        """
        return self._sock is not None

    def connect(self, host: str, port: int) -> None:
        """
        Connect this client to server and maintain the connection.
        """
        if self._sock:
            raise Exception("already connected")
        self._sock = _LoggedSocket(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM),
            logging.getLogger("masenko.client.socket"),
        )
        self._sock.connect((host, port))

    def disconnect(self) -> None:
        """
        Disconnect this client from the server.
        """
        if not self._sock:
            return
        try:
            self.quit()
        except Exception:
            self._log.debug("forcing disconnection")
        finally:
            if self._sock:
                self._sock.close()
                self._sock = None

    def quit(self) -> None:
        """
        Send a QUIT command to the server.
        This should not be necessary as `disconnect` method takes care of the disconnection process
        already.
        """
        verb, payload = self._do("QUIT", None)
        if verb != "OK":
            raise UnexpectedResponseError(verb, payload)

    def ping(self) -> None:
        """
        Send a PING command to the server.
        """
        verb, payload = self._do("PING", None)
        if verb != "PONG":
            raise UnexpectedResponseError(verb, payload)

    @contextmanager
    def atomic(self) -> Iterator[Transaction]:
        """
        Returns a transaction context manager. All operations executed on returned transactions are
        accumulated and executed on the context cleanup.
        """
        tx = Transaction()
        yield tx

        if not len(tx._operations):
            return

        if not self._sock:
            raise Exception("not connected")

        self._sock.sendall(b"ATOMIC\n")
        for request in tx._operations:
            self._sock.sendall(request)
        self._sock.sendall(b"DONE\n")
        resp = self._sock.recv(4096)
        verb, payload = _parse_response(resp)
        if verb == "OK":
            return
        raise UnexpectedResponseError(verb, payload)

    def push(
        self,
        task_name: str,
        payload: Any = None,
        *,
        queue: str = None,
        deadqueue: str = None,
        retry: int = None,
        execute_at: datetime.datetime = None,
    ) -> int:
        """
        Publish a task.
        """
        request: Dict[str, Any] = {"name": task_name}
        if queue:
            request["queue"] = queue
        if deadqueue:
            request["deadqueue"] = deadqueue
        if payload:
            request["payload"] = payload
        if retry is not None:
            request["retry"] = retry
        if execute_at is not None:
            request["execute_at"] = execute_at.isoformat()
        verb, data = self._do("PUSH", request)
        if verb == "OK":
            return data["id"]
        if verb == "ERR":
            raise Error(data)
        raise UnexpectedResponseError(verb, data)

    def fetch(
        self,
        queues: Union[Tuple[str], List[str]] = None,
        *,
        timeout: Union[int, str] = None,
    ) -> Any:
        """
        Pull a single task. This call blocks until a task is retired or timeout
        deadline is reached.
        If deadline is reached, EmptyError is raised.
        """
        request: Dict[str, Any] = {}
        if queues is not None:
            request["queues"] = queues
        if timeout is not None:
            request["timeout"] = timeout

        verb, task = self._do("FETCH", request)
        if verb == "OK":
            return task
        if verb == "EMPTY":
            raise EmptyError()
        raise UnexpectedResponseError(verb, task)

    def ack(self, task_id: int) -> None:
        """
        Acknowledge the task with given ID. Task must be first returned to this
        client as a result of fetch call.
        Once acknowledged, task is forever deleted from the queue.
        """
        verb, payload = self._do("ACK", {"id": task_id})
        if verb == "OK":
            return
        raise UnexpectedResponseError(verb, payload)

    def nack(self, task_id: int) -> None:
        """
        Acknowledge negatively the task with given ID as failed. Task must be
        first given to this client as a result of fetch call.
        Negative acknowledgement will return the task to the queue and reschedule
        it for future consumption.
        """
        verb, payload = self._do("NACK", {"id": task_id})
        if verb == "OK":
            return
        raise UnexpectedResponseError(verb, payload)

    def _do(self, verb: str, payload: Any = None) -> Tuple[str, Any]:
        if not self._sock:
            raise Exception("not connected")

        raw = json.dumps(payload or {})
        data = f"{verb} {raw}\n".encode("utf8")
        # Each request is followed by a response. This is a synchronous process.
        try:
            self._sock.sendall(data)

            # There is no need to buffer received database between requests, because protocol is
            # synchronous.
            recv = self._sock.recv(4096)
            while not recv.endswith(b"\n"):
                recv += self._sock.recv(4096)
                if len(recv) == 0:
                    raise ConnectionError("received no data")
        except ConnectionError as e:
            self._log.debug("disconnecting because of connection error: %s", e)
            self._sock.close()
            self._sock = None
            raise e
        else:
            return _parse_response(recv[:-1])


def _parse_response(raw: bytes) -> Tuple[str, Any]:
    verb, payload = _split_response(raw)
    if verb == "ERR":
        raise ResponseError(payload.get("msg", ""), payload)
    return verb, payload


def _split_response(raw: bytes) -> Tuple[str, Any]:
    try:
        verb, payload = raw.split(None, 1)
    except ValueError:
        return raw.decode("utf8").strip(), None
    if not payload:
        return verb.decode("utf8"), None
    return verb.decode("utf8"), json.loads(payload)


class Error(Exception):
    pass


class EmptyError(Error):
    """
    EmptyError is raised when no result can be returned.
    """

    pass


class UnexpectedResponseError(Error):
    pass


class ResponseError(Error):
    def __init__(self, msg, payload):
        super().__init__(msg)
        self.payload = payload


class _LoggedSocket:
    def __init__(self, sock, log):
        self._log = log
        self._sock = sock

    def connect(self, address):
        self._sock.connect(address)
        self._log.debug("connected to %s", address)

    def close(self):
        self._sock.close()
        self._log.debug("connection closed")

    def recv(self, bufsize) -> bytes:
        data = self._sock.recv(bufsize)
        self._log.debug("received response %s", data)
        return data

    def sendall(self, data: bytes):
        res = self._sock.sendall(data)
        self._log.debug("sending request %s", data)
        return res


@contextmanager
def connect(host: str, port: int, client_cls=Client):
    """
    Returns a context manager that manintains client connection to a Masenko server.
    """
    c = client_cls()
    c.connect(host, port)
    yield c
    c.disconnect()
