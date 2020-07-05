import sys
import unittest
import secrets
import subprocess
import os
import time
import shutil
import random
from unittest import mock

import client


class SimpleTest(unittest.TestCase):
    project_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    )

    def setUp(self):
        self.server_addr = ("localhost", 12000 + random.randrange(1, 1000))

        self._tmp_dir = "/tmp/" + secrets.token_hex(16)
        os.makedirs(self._tmp_dir)

        self._server_process = subprocess.Popen(
            [self.project_dir + "/bin/masenko"],
            stdout=sys.stdout,
            stderr=sys.stderr,
            env={
                "MASENKO_STORE_DIR": self._tmp_dir,
                "MASENKO_LISTEN_TCP": f"localhost:{self.server_addr[1]}",
            },
            cwd="../../",
        )
        time.sleep(0.5)  # Give server time to setup.

    def tearDown(self):
        self._server_process.terminate()
        try:
            self._server_process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            self._server_process.kill()
            self._server_process.wait()

        shutil.rmtree(self._tmp_dir)

    def test_server_down(self):
        queue = random_queue_name()

        with client.connect(*self.server_addr) as c:
            c.ping()

            # Stopping the server must disconnect the client.

            self._server_process.terminate()
            self._server_process.wait()

            for _ in range(30):
                time.sleep(0.3)
                if not c.is_connected():
                    return
            raise Exception("client still connected")

    def test_simple_push_pull(self):
        queue = random_queue_name()

        with client.connect(*self.server_addr) as c:
            c.ping()
            c.push("my-task", queue=queue)
            task = c.fetch(timeout=1, queues=[queue])
            assert task["name"] == "my-task"
            c.nack(task["id"])

            task = c.fetch(timeout="1.2s", queues=[queue])
            assert task["name"] == "my-task"
            c.ack(task["id"])

    def test_transaction(self):
        queue = random_queue_name()

        with client.connect(*self.server_addr) as c:
            c.ping()

            c.push("my-task", queue=queue)
            task = c.fetch(timeout=1, queues=[queue])
            assert task["name"] == "my-task"

            with c.atomic() as tx:
                tx.ack(task["id"])
                for _ in range(10):
                    tx.push("another-task", queue=queue)

    def test_transaction_failure(self):
        queue = random_queue_name()
        with client.connect(*self.server_addr) as c:
            # Transaction commit must fail. Any operation within the transaction must be buffered.
            with self.assertRaises(client.ResponseError):
                with c.atomic() as tx:
                    try:
                        tx.push("a-task", queue=queue)
                        tx.ack(1234567890)
                    except Exception as exc:
                        raise Exception("this must not fail") from exc

            # Failed transaction must not execute any operation.
            with self.assertRaises(client.EmptyError):
                c.fetch(queues=[queue], timeout="1ms")

    @mock.patch.object(client.BareClient, "ping")
    def test_ping_is_sent_in_the_background(self, ping):
        c = client.Client()
        c._heartbeat_sec = 0.1
        try:
            c.connect(*self.server_addr)

            # Several calls expected when heartbeat is so fast.
            for _ in range(10):
                if ping.call_count >= 5:
                    return
                time.sleep(0.1)

            raise Exception(f"ping called only {ping.call_count} time(s)")

        finally:
            c.disconnect()

    def test_ping_can_close_connection(self):
        c = client.Client()
        c._heartbeat_sec = 0.1
        try:
            c.connect(*self.server_addr)

            self._server_process.kill()
            self._server_process.wait()

            time.sleep(0.3)

            self.assertIs(c.is_connected(), False)

            with self.assertRaises(Exception):
                c.ping()

        finally:
            c.disconnect()


def random_queue_name() -> str:
    return secrets.token_hex(8)


def enable_logging():
    import logging

    logging.basicConfig(level=logging.DEBUG, format="%(levelname)6s | %(message)s")


if os.getenv("VERBOSE") in ("1", "t", "T"):
    enable_logging()

if __name__ == "__main__":
    unittest.main()
