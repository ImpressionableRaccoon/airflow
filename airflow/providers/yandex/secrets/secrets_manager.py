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
"""Objects relating to sourcing secrets from Yandex Cloud Lockbox."""
from __future__ import annotations

from functools import cached_property
from typing import Any

import yandex.cloud.lockbox.v1.payload_pb2 as payload_pb
import yandex.cloud.lockbox.v1.payload_service_pb2 as payload_service_pb
import yandex.cloud.lockbox.v1.payload_service_pb2_grpc as payload_service_pb_grpc
import yandex.cloud.lockbox.v1.secret_pb2 as secret_pb
import yandex.cloud.lockbox.v1.secret_service_pb2 as secret_service_pb
import yandex.cloud.lockbox.v1.secret_service_pb2_grpc as secret_service_pb_grpc

from airflow.providers.yandex.hooks.yandex import YandexCloudBaseHook
from airflow.secrets import BaseSecretsBackend


class LockboxSecretBackend(BaseSecretsBackend):
    """
    Retrieves Connection or Variables from Yandex Lockbox.

    Configurable via ``airflow.cfg`` like so:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.yandex.secrets.secrets_manager.SecretsManagerBackend
        backend_kwargs = {"connections_prefix": "airflow/connections"}

    For example, when ``{"connections_prefix": "airflow/connections"}`` is set, if a secret is defined with
    the path ``airflow/connections/smtp_default``, the connection with conn_id ``smtp_default`` would be
    accessible.

    When ``{"variables_prefix": "airflow/variables"}`` is set, if a secret is defined with
    the path ``airflow/variables/hello``, the variable with the name ``hello`` would be accessible.

    When ``{"config_prefix": "airflow/config"}`` is set, if a secret is defined with
    the path ``airflow/config/sql_alchemy_conn``, the config with key ``sql_alchemy_conn`` would be
    accessible.

    When the prefix is empty, keys will use the Lockbox Secrets without any prefix.

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.yandex.secrets.secrets_manager.SecretsManagerBackend
        backend_kwargs = {"yc_connection_id": "<connection_ID>", "folder_id": "<folder_ID>"}

    You need to specify id of yandexcloud connection to connect to Yandex Lockbox with.
    Also, you need to specify the Yandex Cloud folder ID to search for Yandex Lockbox secrets in.

    :param yc_oauth_token: Specifies the user account OAuth token to connect to Yandex Lockbox with.
        Looks like `y3_xxxxx`.
        Either this or a service account JSON or connection_id must be specified.
    :param yc_sa_key_json: Specifies the service account auth JSON.
        Looks like `{"id": "...", "service_account_id": "...", "private_key": "..."}`.
        It will be used instead of the OAuth token and the SA JSON file path field if specified.
    :param yc_sa_key_json_path: Specifies the service account auth JSON file path.
        File content looks like `{"id": "...", "service_account_id": "...", "private_key": "..."}`.
        It will be used instead of the OAuth token if specified.
    :param yc_connection_id: Specifies the connection ID to connect to Yandex Lockbox with.
        If OAuth token and service account auth JSON not specified
        and connection id set to None (null in JSON),
        requests will use default connection_id from YandexCloudBaseHook.
        Default: "yandexcloud_default"
    :param folder_id: Specifies the folder ID to search for Yandex Lockbox secrets in.
        If set to None (null in JSON), requests will use the connection folder_id if specified.
    :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
        If set to None (null in JSON), requests for connections will not be sent to Yandex Lockbox.
        Default: "airflow/connections"
    :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
        If set to None (null in JSON), requests for variables will not be sent to Yandex Lockbox.
        Default: "airflow/variables"
    :param config_prefix: Specifies the prefix of the secret to read to get Configurations.
        If set to None (null in JSON), requests for variables will not be sent to Yandex Lockbox.
        Default: "airflow/config"
    :param sep: Specifies the separator used to concatenate secret_prefix and secret_id.
        Default: "/"
    """

    def __init__(
        self,
        yc_oauth_token: str | None = None,
        yc_sa_key_json: str | None = None,
        yc_sa_key_json_path: str | None = None,
        yc_connection_id: str | None = None,
        folder_id: str | None = None,
        connections_prefix: str | None = "airflow/connections",
        variables_prefix: str | None = "airflow/variables",
        config_prefix: str | None = "airflow/config",
        sep: str = "/",
    ):
        super().__init__()

        self.yc_oauth_token = yc_oauth_token
        self.yc_sa_key_json = yc_sa_key_json
        self.yc_sa_key_json_path = yc_sa_key_json_path
        self.yc_connection_id = yc_connection_id

        self.folder_id = folder_id
        self.connections_prefix = connections_prefix.rstrip(sep) if connections_prefix is not None else None
        self.variables_prefix = variables_prefix.rstrip(sep) if variables_prefix is not None else None
        self.config_prefix = config_prefix.rstrip(sep) if config_prefix is not None else None
        self.sep = sep

    @property
    def _client(self):
        """Create a Yandex Cloud SDK client."""
        return self._base_hook.client

    @cached_property
    def _base_hook(self) -> YandexCloudBaseHook:
        params: dict[str, Any] = {
            "yandex_conn_id": self.yc_connection_id,
            "default_folder_id": self.folder_id,
        }

        if self.yc_oauth_token:
            params["extras"] = {"oauth": self.yc_oauth_token}
        if self.yc_sa_key_json:
            params["extras"] = {"service_account_json": self.yc_sa_key_json}
        if self.yc_sa_key_json_path:
            params["extras"] = {"service_account_json_path": self.yc_sa_key_json_path}

        return YandexCloudBaseHook(**params)

    def get_conn_value(self, conn_id: str) -> str | None:
        """
        Retrieve from Secrets Backend a string value representing the Connection object.

        :param conn_id: connection id
        :return: Connection Value
        """
        if self.connections_prefix is None:
            return None

        if not self.yc_oauth_token and not self.yc_sa_key_json and not self.yc_sa_key_json_path:
            if conn_id == self.yc_connection_id:
                return None

            if not self.yc_connection_id and conn_id == YandexCloudBaseHook.default_conn_name:
                return None

        return self._get_secret_value(self.connections_prefix, conn_id)

    def get_variable(self, key: str) -> str | None:
        """
        Return value for Airflow Variable.

        :param key: Variable Key
        :return: Variable Value
        """
        if self.variables_prefix is None:
            return None

        return self._get_secret_value(self.variables_prefix, key)

    def get_config(self, key: str) -> str | None:
        """
        Return value for Airflow Config Key.

        :param key: Config Key
        :return: Config Value
        """
        if self.config_prefix is None:
            return None

        return self._get_secret_value(self.config_prefix, key)

    def _build_secret_name(self, prefix: str, key: str):
        if len(prefix) == 0:
            return key
        return f"{prefix}{self.sep}{key}"

    def _get_secret_value(self, prefix: str, key: str) -> str | None:
        secret: secret_pb.Secret = None
        for s in self._get_secrets():
            if s.name == self._build_secret_name(prefix=prefix, key=key):
                secret = s
                break
        else:
            return None

        payload = self._get_payload(secret.id, secret.current_version.id)
        entries = {entry.key: entry.text_value for entry in payload.entries if entry.text_value}

        if len(entries) == 0:
            return None
        return sorted(entries.values())[0]

    def _get_secrets(self) -> list[secret_pb.Secret]:
        response = self._list_secrets(folder_id=self._folder_id)

        secrets: list[secret_pb.Secret] = response.secrets[:]
        next_page_token = response.next_page_token
        while next_page_token != "":
            response = self._list_secrets(
                folder_id=self._folder_id,
                page_token=next_page_token,
            )
            secrets.extend(response.secrets)
            next_page_token = response.next_page_token

        return secrets

    def _get_payload(self, secret_id: str, version_id: str) -> payload_pb.Payload:
        request = payload_service_pb.GetPayloadRequest(
            secret_id=secret_id,
            version_id=version_id,
        )
        return self._client(payload_service_pb_grpc.PayloadServiceStub).Get(request)

    def _list_secrets(self, folder_id: str, page_token: str = "") -> secret_service_pb.ListSecretsResponse:
        request = secret_service_pb.ListSecretsRequest(
            folder_id=folder_id,
            page_token=page_token,
        )
        return self._client(secret_service_pb_grpc.SecretServiceStub).List(request)

    @property
    def _folder_id(self):
        return self._base_hook.default_folder_id
