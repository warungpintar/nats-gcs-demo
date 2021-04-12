import asyncio
import signal
import time
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN
from faker import Faker

import holder_pb2


class WriteCardHolder:
    def PromptForAddress(self,card_holder):
        fake = Faker()
        card_holder.name = fake.name()
        card_holder.job = fake.job()
        card_holder.phone_number = fake.phone_number()
        card_holder.address = fake.address()
        card_holder.card_number = fake.credit_card_number()
        card_holder.card_provider = fake.credit_card_provider()
        return card_holder


async def run(loop):
    nc = NATS()
    await nc.connect(servers=["nats://localhost:4222"], io_loop=loop)

    sc = STAN()
    await sc.connect("test-cluster", "dataeng-publisher", nats=nc)

    # sebelumnya kita publish angka saja
    # i = 0
    # while True:
    #     print(f"publish {i}")
    #     await sc.publish("hitung", str(i).encode())
    #     i += 1
    #     time.sleep(1)

    # lalu kita ingin publish message dalam bentuk protocol buffers
    write_card_holder = WriteCardHolder()
    while True:
        card_holder_book = holder_pb2.CardHolder()
        msg = write_card_holder.PromptForAddress(card_holder_book)
        print("Publish", msg)
        await sc.publish("hitung", msg)
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
