# Copyright 2019, Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import threading
import time

from google.cloud import pubsub_v1


script_dir = os.path.dirname(os.path.realpath(__file__))
log_filename = os.path.join(script_dir, "subscriber.log")

log_format = (
    "%(levelname)-8s [%(asctime)s] %(threadName)-33s "
    "[%(name)s] [%(filename)s:%(lineno)d][%(funcName)s] %(message)s"
)
log_format = (
    "%(levelname)-8s [%(asctime)s] %(threadName)-33s "
    "[%(funcName)s] %(message)s"
)

logging.basicConfig(
    level=logging.DEBUG,
    format=log_format,
)

# TOOD: fix docstring in one of the codes...message? leaser? invalid docstring...
logger = logging.getLogger(__name__)

PROJECT_NAME = "precise-truck-742"
TOPIC_NAME = "iss-7677-events"
SUBSCRIPTION_NAME = f"subscription-{TOPIC_NAME}"

pending_ack = 0  # the number of received, but not yet ACK-ed messages

pending_ack_lock = threading.Lock()


def callback(message):
    """A callback when a message is received."""
    global pending_ack
    global pending_ack_lock

    content = message.data.decode("latin-1")

    with pending_ack_lock:
        pending_ack += 1
        logging.info (
            f"\x1b[1;33m Received Message: {content} (pending ACK {pending_ack})\x1b[0m"
        )
    time.sleep(10)

    with pending_ack_lock:
        pending_ack -= 1
        logging.info (
            f"\x1b[38;5;214m Will ACK Message: {content} (pending ACK {pending_ack})\x1b[0m")
        message.ack()


def main():
    subscriber = pubsub_v1.SubscriberClient()

    subscription_path = subscriber.subscription_path(PROJECT_NAME, SUBSCRIPTION_NAME)
    topic_path = subscriber.topic_path(PROJECT_NAME, TOPIC_NAME)
    # subscriber.create_subscription(name=subscription_path, topic=topic_path)

    logging.info(f"Subscriber started for {subscription_path}")
    flow_control = pubsub_v1.types.FlowControl(max_messages=5)
    subscriber.subscribe(
        subscription_path, callback=callback, flow_control=flow_control)

    while True:
        try:
            time.sleep(60)
        except KeyboardInterrupt:
           logging.info("Interrupted, exiting...")
           break


if __name__ == '__main__':
    main()
