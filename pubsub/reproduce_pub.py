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

from datetime import datetime
import itertools
import logging
import os
import random
import time

from google.cloud import pubsub_v1


script_dir = os.path.dirname(os.path.realpath(__file__))
log_filename = os.path.join(script_dir, "publisher.log")

log_format = (
    "%(levelname)-8s [%(asctime)s] %(threadName)-33s "
    "[%(name)s] [%(filename)s:%(lineno)d][%(funcName)s] %(message)s"
    # TODO: handlers!
)

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
)

logger = logging.getLogger(__name__)

PROJECT_NAME = "precise-truck-742"
TOPIC_NAME = "iss-7677-events"


def make_messages(msg_counter):
    messages = []
    to_create = random.randint(1, 10)

    for i in range(to_create):
        time_str = datetime.now().strftime("%H:%M:%S")
        count = next(msg_counter)
        msg = f"this is message {count:0>5} [{time_str}, {i+1}/{to_create}]"
        messages.append(msg)

    return messages


def main():
    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(PROJECT_NAME, TOPIC_NAME)
    # publisher.create_topic(topic_path)

    msg_counter = itertools.count(start=1)

    while True:
        messages = make_messages(msg_counter)  
        logger.info(f"Publishing {len(messages)} message(s): {messages}")

        try:
            for msg in messages:
                publisher.publish(topic_path, msg.encode("utf-8"))
        except Exception:
            logger.exception("Error publishing messages")

        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            logger.info("Interrupted, aborting...")
            break


if __name__ == "__main__":
    main()
