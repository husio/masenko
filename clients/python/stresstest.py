import client
import time
import collections
import multiprocessing
import argparse

from typing import List


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--processes", default=2, type=int, action="store")
    parser.add_argument("-b", "--batchsize", default=100, type=int, action="store")
    args = parser.parse_args()

    clients = []
    for _ in range(args.processes):
        p = multiprocessing.Process(target=run, args=(args.batchsize,))
        clients.append(p)

    for p in clients:
        p.start()

    for p in clients:
        p.join()


def run(batch_size: int):
    with client.connect("localhost", 12345) as c:
        queue_name = "pyq"
        to_ack: List[int] = []

        while True:
            c.ping()

            start = time.time()

            with c.transaction() as tx:
                payload = {
                    "something_id": 1241244,
                    "client_id": 104922,
                    "user_id": 91492,
                    "recover": True,
                    "text": "In publishing and graphic design, Lorem ipsum is a placeholder text"
                    "commonly used to demonstrate the visual form of a document or a typeface without"
                    "relying on meaningful content",
                    "now": start,
                }
                for _ in range(batch_size):
                    tx.push("my-task", payload, queue=queue_name)

                for id in to_ack:
                    tx.ack(id)
                to_ack = []

            for _ in range(254 // 2):
                try:
                    task = c.fetch(timeout=1, queues=[queue_name])
                    to_ack.append(task["id"])
                except client.EmptyError:
                    break

            work_time_ms = int((time.time() - start) * 1000)
            # print(f"{batch_size} messages in {work_time_ms}ms")


if __name__ == "__main__":
    main()
