import asyncio
import functools
import hashlib
import os
import time
from datetime import datetime
from google.cloud import storage
from pathlib import Path


os.environ["NATS_CHANNEL"] = "hitung"
os.environ["DATA_PATH"] = os.path.join(os.getcwd(), "data_tmp")
os.environ["GCS_PATH"] = "warpin-datalake"
os.environ["GCS_SUBPATH"] = "nats-lake"


def write_to_gcs(path, gcs_path): 
    client = storage.Client()
    bucket = client.bucket(os.environ.get('GCS_PATH'))
    blob = storage.Blob(gcs_path, bucket)
    if Path(path).stat().st_size > 0:
        blob.upload_from_filename(path)

def get_name():

    channel = os.environ.get('NATS_CHANNEL')
    base_path = os.environ.get('DATA_PATH')

    date = datetime.today()
    datestr = date.strftime('%Y%m%d')
    datetimestr = date.strftime('%Y%m%d-%H%M')
    md5date = (hashlib.md5(datestr.encode())).hexdigest()
    md5datetime = (hashlib.md5(datetimestr.encode())).hexdigest()

    path = os.path.join(base_path, channel, f"{md5date[:6]}-{datestr}")
    mv_path = os.path.join(channel, f"{md5date[:6]}-{datestr}")

    os.makedirs(path, exist_ok=True)

    filename = os.path.join(path, f"{md5datetime[:6]}-{datetimestr}.json")
    mv_filename = os.path.join(mv_path, f"{md5datetime[:6]}-{datetimestr}.json")

    return filename, mv_filename

async def run(loop):

	filename, mv_filename = get_name()
	i = 0

	value = os.stat(filename)[8]
	time.sleep(1)
	while True:

		print(f"count {i}")
		old_value, value = value, os.stat(filename)[8]

		if value == old_value: 
	
			prefix = os.environ.get('GCS_SUBPATH')

			gcs_path = prefix + "/" + mv_filename

			write_to_gcs(filename, gcs_path)

			os.remove(filename)

			filename, mv_filename = get_name()
			time.sleep(1)
		
		i += 1
		time.sleep(1)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()               