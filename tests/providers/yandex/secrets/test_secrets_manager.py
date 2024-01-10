# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json
from unittest.mock import Mock, patch

import yandex.cloud.lockbox.v1.payload_pb2 as payload_pb
import yandex.cloud.lockbox.v1.secret_pb2 as secret_pb
import yandex.cloud.lockbox.v1.secret_service_pb2 as secret_service_pb

from airflow.providers.yandex.hooks.yandex import YandexCloudBaseHook
from airflow.providers.yandex.secrets.secrets_manager import LockboxSecretBackend


class TestSecretsManagerBackend:
    @patch("airflow.hooks.base.BaseHook.get_connection")
    @patch("airflow.providers.yandex.hooks.yandex.YandexCloudBaseHook._get_credentials")
    def test_yandex_lockbox_secrets_manager__client(self, mock_get_credentials, mock_get_connection):
        mock_get_credentials.return_value = {"token": 122323}
        mock_get_connection.return_value = Mock(
            connection_id=YandexCloudBaseHook.default_conn_name,
            extra_dejson={"folder_id": "folder_id"},
        )

        sm = LockboxSecretBackend()
        client = sm._client
        assert client == sm._base_hook.client

    @patch("airflow.hooks.base.BaseHook.get_connection")
    @patch("airflow.providers.yandex.hooks.yandex.YandexCloudBaseHook._get_credentials")
    def test_yandex_lockbox_secrets_manager__base_hook(self, mock_get_credentials, mock_get_connection):
        mock_get_credentials.return_value = {"token": 122323}
        mock_get_connection.return_value = Mock(
            connection_id=YandexCloudBaseHook.default_conn_name,
            extra_dejson={"folder_id": "folder_id"},
        )

        hook = LockboxSecretBackend()._base_hook
        assert hook is not None

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secrets_manager_get_connection(self, mock_get_value):
        uri = "scheme://user:pass@host:100"
        mock_get_value.return_value = uri
        conn = LockboxSecretBackend().get_connection("fake_conn")
        assert conn.conn_id == "fake_conn"
        assert conn.conn_type == "scheme"
        assert conn.host == "host"
        assert conn.schema == ""
        assert conn.login == "user"
        assert conn.port == 100
        assert conn.get_uri() == uri

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secrets_manager_get_connection_from_json(self, mock_get_value):
        conn_id = "airflow_to_yandexcloud"
        conn_type = "yandex_cloud"
        extra = "some extra values"
        c = {
            "conn_type": conn_type,
            "extra": extra,
        }
        mock_get_value.return_value = json.dumps(c)
        conn = LockboxSecretBackend().get_connection(conn_id)
        assert conn.conn_id == conn_id
        assert conn.conn_type == conn_type
        assert conn.extra == extra

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secrets_manager_get_variable(self, mock_get_value):
        k = "thisiskey"
        v = "thisisvalue"
        mock_get_value.return_value = v
        value = LockboxSecretBackend().get_variable(k)
        assert value == v

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secrets_manager_get_config(self, mock_get_value):
        k = "thisiskey"
        v = "thisisvalue"
        mock_get_value.return_value = v
        value = LockboxSecretBackend().get_config(k)
        assert value == v

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secrets_manager_get_connection_prefix_is_none(self, mock_get_value):
        uri = "scheme://user:pass@host:100"
        mock_get_value.return_value = uri
        conn = LockboxSecretBackend(
            connections_prefix=None,
        ).get_connection("fake_conn")
        assert conn is None

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secrets_manager_get_connection_conn_id_for_backend(self, mock_get_value):
        conn_id = "yandex_cloud"
        uri = "scheme://user:pass@host:100"
        mock_get_value.return_value = uri
        conn = LockboxSecretBackend(
            yc_connection_id=conn_id,
        ).get_connection(conn_id)
        assert conn is None

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secrets_manager_get_connection_default_conn_id(self, mock_get_value):
        conn_id = YandexCloudBaseHook.default_conn_name
        uri = "scheme://user:pass@host:100"
        mock_get_value.return_value = uri
        conn = LockboxSecretBackend().get_connection(conn_id)
        assert conn is None

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secrets_manager_get_variable_prefix_is_none(self, mock_get_value):
        k = "thisiskey"
        v = "thisisvalue"
        mock_get_value.return_value = v
        value = LockboxSecretBackend(
            variables_prefix=None,
        ).get_variable(k)
        assert value is None

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secrets_manager_get_config_prefix_is_none(self, mock_get_value):
        k = "thisiskey"
        v = "thisisvalue"
        mock_get_value.return_value = v
        value = LockboxSecretBackend(
            config_prefix=None,
        ).get_config(k)
        assert value is None

    def test_yandex_lockbox_secrets_manager__build_secret_name(self):
        prefix = "thiisprefix"
        key = "thisiskey"
        res = LockboxSecretBackend()._build_secret_name(prefix, key)
        assert res == "thiisprefix/thisiskey"

    def test_yandex_lockbox_secrets_manager__build_secret_name_no_prefix(self):
        prefix = ""
        key = "thisiskey"
        res = LockboxSecretBackend()._build_secret_name(prefix, key)
        assert res == "thisiskey"

    def test_yandex_lockbox_secrets_manager__build_secret_name_custom_sep(self):
        prefix = "thiisprefix"
        key = "thisiskey"
        res = LockboxSecretBackend(
            sep="_",
        )._build_secret_name(prefix, key)
        assert res == "thiisprefix_thisiskey"

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_secrets")
    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_payload")
    def test_yandex_lockbox_secrets_manager__get_secret_value(self, mock_get_payload, mock_get_secrets):
        target_name = "target_name"
        target_text = "target_text"

        mock_get_secrets.return_value = [
            secret_pb.Secret(
                id="123",
                name="one",
            ),
            secret_pb.Secret(
                id="456",
                name=target_name,
            ),
            secret_pb.Secret(
                id="789",
                name="two",
            ),
        ]
        mock_get_payload.return_value = payload_pb.Payload(
            entries=[
                payload_pb.Payload.Entry(text_value=target_text),
            ],
        )

        res = LockboxSecretBackend()._get_secret_value("", target_name)
        assert res == target_text

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_secrets")
    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_payload")
    def test_yandex_lockbox_secrets_manager__get_secret_value_not_found(
        self, mock_get_payload, mock_get_secrets
    ):
        target_name = "target_name"
        target_text = "target_text"

        mock_get_secrets.return_value = [
            secret_pb.Secret(
                id="123",
                name="one",
            ),
            secret_pb.Secret(
                id="789",
                name="two",
            ),
        ]
        mock_get_payload.return_value = payload_pb.Payload(
            entries=[
                payload_pb.Payload.Entry(text_value=target_text),
            ],
        )

        res = LockboxSecretBackend()._get_secret_value("", target_name)
        assert res is None

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_secrets")
    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._get_payload")
    def test_yandex_lockbox_secrets_manager__get_secret_value_no_text_entries(
        self, mock_get_payload, mock_get_secrets
    ):
        target_name = "target_name"
        target_value = b"01010101"

        mock_get_secrets.return_value = [
            secret_pb.Secret(
                id="123",
                name="one",
            ),
            secret_pb.Secret(
                id="456",
                name="two",
            ),
            secret_pb.Secret(
                id="789",
                name=target_name,
            ),
        ]
        mock_get_payload.return_value = payload_pb.Payload(
            entries=[
                payload_pb.Payload.Entry(binary_value=target_value),
            ],
        )

        res = LockboxSecretBackend()._get_secret_value("", target_name)
        assert res is None

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._folder_id")
    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._list_secrets")
    def test_yandex_lockbox_secrets_manager__get_secrets(self, mock_list_secrets, mock_folder_id):
        secrets = secret_service_pb.ListSecretsResponse(
            secrets=[
                secret_pb.Secret(
                    id="123",
                ),
                secret_pb.Secret(
                    id="456",
                ),
            ],
        )
        mock_list_secrets.return_value = secrets
        mock_folder_id.return_value = "someid"

        res = LockboxSecretBackend()._get_secrets()
        assert res == secrets.secrets

    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._folder_id")
    @patch("airflow.providers.yandex.secrets.secrets_manager.LockboxSecretBackend._list_secrets")
    def test_yandex_lockbox_secrets_manager__get_secrets_page_token(self, mock_list_secrets, mock_folder_id):
        mock_folder_id.return_value = "someid"

        first_secrets = secret_service_pb.ListSecretsResponse(
            secrets=[
                secret_pb.Secret(
                    id="123",
                ),
                secret_pb.Secret(
                    id="456",
                ),
            ],
            next_page_token="token",
        )
        second_secrets = secret_service_pb.ListSecretsResponse(
            secrets=[
                secret_pb.Secret(
                    id="789",
                ),
                secret_pb.Secret(
                    id="000",
                ),
            ],
            next_page_token="",
        )
        mock_list_secrets.side_effect = [
            first_secrets,
            second_secrets,
        ]

        res = LockboxSecretBackend()._get_secrets()
        assert res == [*first_secrets.secrets, *second_secrets.secrets]

    @patch("airflow.hooks.base.BaseHook.get_connection")
    @patch("airflow.providers.yandex.hooks.yandex.YandexCloudBaseHook._get_credentials")
    def test_yandex_lockbox_secrets_manager__folder_id(self, mock_get_credentials, mock_get_connection):
        folder_id = "id1"

        mock_get_credentials.return_value = {"token": 122323}
        mock_get_connection.return_value = Mock(
            connection_id=YandexCloudBaseHook.default_conn_name,
            extra_dejson={"folder_id": "folder_id"},
        )

        res = LockboxSecretBackend(
            folder_id=folder_id,
        )._folder_id
        assert res == folder_id

    @patch("airflow.hooks.base.BaseHook.get_connection")
    @patch("airflow.providers.yandex.hooks.yandex.YandexCloudBaseHook._get_credentials")
    def test_yandex_lockbox_secrets_manager__folder_id_from_connection(
        self, mock_get_credentials, mock_get_connection
    ):
        folder_id = "id1"

        mock_get_credentials.return_value = {"token": 122323}
        mock_get_connection.return_value = Mock(
            connection_id=YandexCloudBaseHook.default_conn_name,
            extra_dejson={"folder_id": folder_id},
        )

        res = LockboxSecretBackend()._folder_id
        assert res == folder_id
