FROM python:3.7-slim

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update -y

COPY . /app

RUN pip install --no-cache-dir \
    google-cloud-storage==1.31.2 \
    asyncio-nats-streaming==0.4.0
