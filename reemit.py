#!/usr/bin/env python3
import pika
import sys, os
from pathlib import Path, PurePath

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', virtual_host="Red_t000000015"))

directory = "rabbitmq-storage-recoverer/testidx/topic"

channel = connection.channel()
channel.confirm_delivery()
channel.exchange_declare(exchange='topic', durable=True, exchange_type="topic")
p = Path(directory)
for d in p.iterdir():
    if d.is_dir():
        print(f"Directory - {d} ")
        for filename in os.listdir(d):
            f = os.path.join(directory, d, filename)
            if os.path.isfile(f):
                purepath = PurePath(f)
                queue = purepath.parent.name
                print(f"Queue - {queue}")
                print(f"Handling - {f}")
                channel.queue_declare(queue=queue, durable=True)
                channel.queue_bind(queue=queue, exchange='topic') #, routing_key='my.*')
                msg = open(f, "r")
                content = msg.read()
                msg.close()
                channel.basic_publish(exchange='topic',
                    properties=pika.BasicProperties(content_type='application/json',
                            delivery_mode=1),
                    mandatory=True,
                    routing_key=queue,
                    body=content)
                print(f"msg sent to - {queue}")
connection.close()
