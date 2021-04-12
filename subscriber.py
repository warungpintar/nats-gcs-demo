import asyncio
import functools
import hashlib
import os
import signal
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN
from datetime import datetime

import holder_pb2

os.environ["NATS_SERVERS"]="nats://localhost:4222"
os.environ["NATS_CLUSTER_ID"]="test-cluster"
os.environ["NATS_CHANNEL"]="hitung"
os.environ["DATA_PATH"]=os.path.join(os.getcwd(), "data_tmp")


class ReadCardHolder:
    def __init__(self):
        self.card_holder = holder_pb2.CardHolder()

    def GetRowToInsert(self):
        row_to_insert = {
            "name": self.card_holder.name,
            "job": self.card_holder.job,
            "phone_number": self.card_holder.phone_number,
            "address": self.card_holder.address,
            "card_number": self.card_holder.card_number,
            "card_provider": self.card_holder.card_provider
        }
        return row_to_insert


async def run(loop):
    nc = NATS()
    nats_servers = [s.strip() for s in os.environ.get('NATS_SERVERS').split(',')]
    await nc.connect(servers=nats_servers, io_loop=loop)

    sc = STAN()
    await sc.connect(os.environ.get('NATS_CLUSTER_ID'), "dataeng-subscriber", nats=nc)

    channel = os.environ.get('NATS_CHANNEL')
    queue = f"dataeng_queue_{channel}_gcs"
    durable = f"dataeng_durable_{channel}_gcs"
    base_path = os.environ.get('DATA_PATH')

    async def cb(msg):
        date = datetime.today()
        datestr = date.strftime('%Y%m%d')
        datetimestr = date.strftime('%Y%m%d-%H%M')
        md5date = (hashlib.md5(datestr.encode())).hexdigest()
        md5datetime = (hashlib.md5(datetimestr.encode())).hexdigest()

        path = os.path.join(base_path, channel, f"{md5date[:6]}-{datestr}")
        os.makedirs(path, exist_ok=True)
        filename = os.path.join(path, f"{md5datetime[:6]}-{datetimestr}.json")
        print("Received a message (seq={}): {}".format(msg.seq, msg.data))

        f = open(filename, 'a')
        read_card_holder = ReadCardHolder()
        read_card_holder.card_holder.ParseFromString(msg.data)
        js = json.dumps(read_card_holder.GetRowToInsert())
        f.write(js + "\n")
        f.close()

    await sc.subscribe(channel, queue=queue, start_at="first", durable_name=durable, cb=cb)

    async def signal_handler(loop):
        if nc.is_closed:
            return
        print('Disconnecting...')
        await sc.close()
        await nc.close()

        tasks = [task for task in asyncio.Task.all_tasks() if task is not asyncio.tasks.Task.current_task()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        print('finished awaiting tasks, results: {0}'.format(results))
        loop.stop()

    for sig in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, sig), functools.partial(asyncio.ensure_future, signal_handler(loop)))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()
