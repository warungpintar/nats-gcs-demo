import asyncio
import signal
import time
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN


async def run(loop):
    nc = NATS()
    await nc.connect(servers=["nats://localhost:4222"], io_loop=loop)

    sc = STAN()
    await sc.connect("test-cluster", "dataeng-publisher", nats=nc)

    i = 0
    while True:
        print(f"publish {i}")
        await sc.publish("hitung", str(i).encode())
        i += 1
        time.sleep(1)

    def signal_handler():
        if nc.is_closed:
            return
        print('Disconnecting...')
        loop.create_task(sc.close())
        loop.create_task(nc.close())

    for sig in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, sig), signal_handler)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()
