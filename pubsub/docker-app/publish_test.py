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

import itertools
import logging
from logging import handlers
import os
import random
import time

from google.cloud import pubsub_v1


script_dir = os.path.dirname(os.path.realpath(__file__))
log_filename = os.path.join(script_dir, "logs", "publisher.log")

handler = handlers.TimedRotatingFileHandler(
    filename=log_filename, when="midnight", utc=True
)
log_format = (
    "%(levelname)-8s [%(asctime)s] %(threadName)-33s "
    "[%(name)s] [%(filename)s:%(lineno)d][%(funcName)s] %(message)s"
)
logging.basicConfig(
    level=logging.DEBUG,
    format=log_format,
    handlers=[handler],
)

logger = logging.getLogger(__name__)


def make_messages(msg_counter):
    result = []

    # normally only create a single message, but every now and then randomly
    # create a whole bunch of messages
    create_count = 1 if random.random() < 0.9 else 10

    for _ in range(create_count):
        animal = random.choice(("dog", "cat", "cow", "fish", "pig", "lion"))
        sound = random.choice(("meow", "woof", "moo", "blurp", "oink", "roar"))
        count = next(msg_counter)
        message = f"{animal} says {sound} (msg #{count:0>7})"
        result.append(message)

    return result


def main():
    logger.info(f"main")
    publisher = pubsub_v1.PublisherClient()

    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    topic = 'ANIMAL_EVENTS'
    topic_name = f'projects/{project_id}/topics/{topic}'

    msg_counter = itertools.count(start=1)

    while True:
        messages = make_messages(msg_counter)
        msg_concat = "|".join(messages)
        logger.info(f"Publishing {len(messages)} message(s): {msg_concat}")

        for msg in messages:
            publisher.publish(topic_name, msg.encode("utf-8"))

        sleep_for = random.random() * 10
        try:
            time.sleep(sleep_for)
        except KeyboardInterrupt:
            logger.info(f"Interrupted, aborting...")
            break


if __name__ == "__main__":
    main()
