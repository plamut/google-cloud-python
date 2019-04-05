# -*- coding: utf-8 -*-
#
# Copyright 2019 Google LLC
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

import os
import time

import mock
import pytest

from google.api_core import exceptions
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.gapic import publisher_client_config
from google.cloud.pubsub_v1.proto import pubsub_pb2


class TestSystemPublisher(object):
    def test_list_topics(self):
        project_id = os.environ["PROJECT_ID"]

        client = pubsub_v1.PublisherClient()
        project = client.project_path(project_id)
        response = client.list_topics(project)

    def test_no_account_no_retry_timeout(self):
        script_dir = os.path.dirname(os.path.realpath(__file__))
        config_file = os.path.join(
            script_dir, "assets", "project-credentials-invalid.json"
        )

        messaging_config = publisher_client_config.config["interfaces"][
            "google.pubsub.v1.Publisher"
        ]["retry_params"]["messaging"]

        with mock.patch.dict(messaging_config, total_timeout_millis=10000):
            client = pubsub_v1.PublisherClient.from_service_account_json(config_file)

        project_id = os.environ["PROJECT_ID"]
        topic_path = client.topic_path(project_id, "topic_foo")
        publish_future = client.publish(topic_path, data=b"Hi there!")

        # If the client tries to retry the failed request, the assertion below
        # will fail with a timeout (RefreshError).
        with pytest.raises(exceptions.ServiceUnavailable):
            publish_future.result()
