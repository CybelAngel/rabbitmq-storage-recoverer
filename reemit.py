#!/usr/bin/env python3

## Read files as exported messages from recovered rabbitmq mnesia database and reemits them.
## Also creates a "completed" directory, and COPIES messages there, which allows for resuming
## from the last message it successfully sent

import pika, traceback
import sys, os
import shutil
from pathlib import Path, PurePath
from time import sleep

## Rabbit Settings
rabbitmq_host = '192.168.101.110'
virtual_host = "xxx"
exchange_name = "topic"

## Source data folder settings
base_path = "/home/xxx/rabbit_test"
# source_directory = f"recovered/persistent/{exchange_name}""
source_directory = f"recovered/transient/{exchange_name}"
completed_directory = "completed"

try:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host, virtual_host=virtual_host))
except Exception:
    connection = None
    print("Unable to connect to rabbitmq, check your params, auth ,etc, etc")
    print(traceback.format_exc())

p = Path(os.path.join(base_path, source_directory))
print(f"Starting Path - {p}")
for d in p.iterdir():
    if d.is_dir():
        purepath = PurePath(d)
        queue = purepath.name
        completed_dir = os.path.join(base_path, completed_directory, source_directory, queue)
        try:
            os.makedirs(completed_dir)
        except:
            pass # might already exist
        print(f"Directory - {d} ")
        print(f"Opening channel")
        try:
            if connection:
                channel = connection.channel()
                channel.confirm_delivery()
                channel.exchange_declare(exchange=exchange_name, durable=True, exchange_type="topic")
                channel.queue_declare(queue=queue, durable=True)
                channel.queue_bind(queue=queue, exchange=exchange_name) #, routing_key='my.*')
                bind_success = True
            else:
                print("No connection available, check rabbitmq connection settings")
                bind_success = False
        except Exception:
            print("Unable to create channel, connection disappeared")
            print(traceback.format_exc())
            exit(1)
        if bind_success:
            for filename in os.listdir(d):
                f = os.path.join(d, filename)
                print(f"File - {f}")
                if os.path.isfile(f):
                    fn = Path(f)
                    print(f"Queue - {queue}")
                    print(f"Handling - {fn.name}")
                    check_done = os.path.join(base_path, completed_directory, source_directory, queue, fn.name)
                    if os.path.exists(check_done):
                        print(f"Already processed, skipping - {fn.name}")
                        # sleep(0.1)
                    else:
                        msg = open(f, "r")
                        content = msg.read()
                        msg.close()
                        print(f"Sending - {fn.name}")
                        try:
                            channel.basic_publish(exchange=exchange_name,
                                properties=pika.BasicProperties(content_type='application/json',
                                        delivery_mode=1),
                                mandatory=True,
                                routing_key=queue,
                                body=content)
                            print(f"Marking as complete - {queue}")
                            shutil.copy2(f, completed_dir)
                        except:
                            print(f"Failed - {fn.name}")
        else:
            print("Could not create queue binding, aborting")
            exit(1)
        try:
            print(f"Closing channel")
            channel.close()
        except:
            print("Could not close channel, aborting")
            print("Did you crash the server?")
            exit(1)
print("Closing connection")
try:
    connection.close()
except:
    print("Could not close connection")
    pass
