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
from logging import handlers
import os
import time

from google.cloud import pubsub_v1


script_dir = os.path.dirname(os.path.realpath(__file__))
log_filename = os.path.join(script_dir, "logs", "subscriber.log")

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


def main():
    logger.info(f"main")
    subscriber = pubsub_v1.SubscriberClient()

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    topic = "ANIMAL_EVENTS"
    topic_name = f"projects/{project_id}/topics/{topic}"

    sub = "ANIMAL_SOUNDS_NEWS"
    subscription_name = f"projects/{project_id}/subscriptions/{sub}"

    def callback(message):
        msg = message.data.decode("latin-1")  # latin-1 will not fail
        logger.info(f"Received message {msg}")
        message.ack()

    logger.info(f"Subscribing to subscription {sub}")
    future = subscriber.subscribe(subscription_name, callback)

    while True:
        try:
            logger.info(f"Awaiting future result...")
            future.result()
        except KeyboardInterrupt:
            logger.info(f"Interupted, aborting...")
            future.cancel()
            break
        except Exception:
            logger.exception("Error obtaining future result, will sleep 10 seconds")
            time.sleep(10)


if __name__ == "__main__":
    main()
