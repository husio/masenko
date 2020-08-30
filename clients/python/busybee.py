import client
import time
import collections
import multiprocessing
import random

from typing import List


queues = ("highprio", "lowprio", "default", "registration", "logging", "system")

def main():
    while True:
        try:
            run()
        except Exception:
            time.sleep(1)


def run():
    clients = []
    for queue_name in queues:
        p = multiprocessing.Process(target=run_one, args=(queue_name,))
        clients.append(p)

    for p in clients:
        p.start()

    for p in clients:
        p.join()

def run_one(queue_name : str):
    with client.connect("localhost", 12345) as c:
        to_ack: List[int] = []

        while True:
            time.sleep(0.5)

            c.ping()
            with c.transaction() as tx:
                tx.push("my-task", None, queue=queue_name)

                while len(to_ack) > 20 or (to_ack and random.randint(0, 10) > 4):
                    tx.ack(to_ack.pop())

            if to_ack and random.randint(0, 100) < 5:
                c.nack(to_ack.pop())

            while random.randint(0, 10) > 4:
                try:
                    task = c.fetch(timeout=1, queues=[queue_name])
                    to_ack.append(task["id"])
                except client.EmptyError:
                    pass



if __name__ == "__main__":
    main()
