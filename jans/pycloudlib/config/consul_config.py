"""
jans.pycloudlib.config.consul_config
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains config adapter class to interact with Consul.
"""

import logging
import os
from typing import (
    Any,
    Tuple,
    Union,
)

from consul import Consul

from jans.pycloudlib.config.base_config import BaseConfig
from jans.pycloudlib.utils import (
    as_boolean,
    safe_value,
)

logger = logging.getLogger(__name__)


class ConsulConfig(BaseConfig):
    """This class interacts with Consul backend.


    The following environment variables are used to instantiate the client:

    - ``JANS_CONFIG_CONSUL_HOST``
    - ``JANS_CONFIG_CONSUL_PORT``
    - ``JANS_CONFIG_CONSUL_CONSISTENCY``
    - ``JANS_CONFIG_CONSUL_SCHEME``
    - ``JANS_CONFIG_CONSUL_VERIFY``
    - ``JANS_CONFIG_CONSUL_CACERT_FILE``
    - ``JANS_CONFIG_CONSUL_CERT_FILE``
    - ``JANS_CONFIG_CONSUL_KEY_FILE``
    - ``JANS_CONFIG_CONSUL_TOKEN_FILE``
    """

    def __init__(self):
        self.settings = {
            k: v
            for k, v in os.environ.items()
            if k.isupper() and k.startswith("JANS_CONFIG_CONSUL_")
        }

        self.settings.setdefault(
            "JANS_CONFIG_CONSUL_HOST", "localhost",
        )

        self.settings.setdefault(
            "GLUU_CONFIG_CONSUL_PORT", 8500,
        )

        self.settings.setdefault(
            "GLUU_CONFIG_CONSUL_CONSISTENCY", "stale",
        )

        self.settings.setdefault(
            "GLUU_CONFIG_CONSUL_SCHEME", "http",
        )

        self.settings.setdefault(
            "GLUU_CONFIG_CONSUL_VERIFY", False,
        )

        self.settings.setdefault(
            "GLUU_CONFIG_CONSUL_CACERT_FILE", "/etc/certs/consul_ca.crt",
        )

        self.settings.setdefault(
            "GLUU_CONFIG_CONSUL_CERT_FILE", "/etc/certs/consul_client.crt",
        )

        self.settings.setdefault(
            "GLUU_CONFIG_CONSUL_KEY_FILE", "/etc/certs/consul_client.key",
        )

        self.settings.setdefault(
            "GLUU_CONFIG_CONSUL_TOKEN_FILE", "/etc/certs/consul_token",
        )

        self.prefix = "gluu/config/"
        cert, verify = self._verify_cert(
            self.settings["GLUU_CONFIG_CONSUL_SCHEME"],
            self.settings["GLUU_CONFIG_CONSUL_VERIFY"],
            self.settings["GLUU_CONFIG_CONSUL_CACERT_FILE"],
            self.settings["GLUU_CONFIG_CONSUL_CERT_FILE"],
            self.settings["GLUU_CONFIG_CONSUL_KEY_FILE"],
        )

        self._request_warning(self.settings["GLUU_CONFIG_CONSUL_SCHEME"], verify)

        self.client = Consul(
            host=self.settings["GLUU_CONFIG_CONSUL_HOST"],
            port=self.settings["GLUU_CONFIG_CONSUL_PORT"],
            token=self._token_from_file(self.settings["GLUU_CONFIG_CONSUL_TOKEN_FILE"]),
            scheme=self.settings["GLUU_CONFIG_CONSUL_SCHEME"],
            consistency=self.settings["GLUU_CONFIG_CONSUL_CONSISTENCY"],
            verify=verify,
            cert=cert,
        )

    def _merge_path(self, key: str) -> str:
        """Add prefix to the key.

        For example, given the prefix is ``gluu/config`` and key ``random``,
        calling this method returns ``gluu/config/random`` key.

        :params key: Key name as relative path.
        :returns: Absolute path to prefixed key.
        """
        return "".join([self.prefix, key])

    def _unmerge_path(self, key: str) -> str:
        """Remove prefix from the key.

        For example, given the prefix is ``gluu/config`` and an absolute path
        ``gluu/config/random``, calling this method returns ``random`` key.

        :params key: Key name as relative path.
        :returns: Relative path to key.
        """
        return key[len(self.prefix):]

    def get(self, key: str, default: Any = None) -> Any:
        """Get value based on given key.

        :params key: Key name.
        :params default: Default value if key is not exist.
        :returns: Value based on given key or default one.
        """
        _, result = self.client.kv.get(self._merge_path(key))
        if not result:
            return default
        # this is a bytes
        return result["Value"].decode()

    def set(self, key: str, value: Any) -> bool:
        """Set key with given value.

        :params key: Key name.
        :params value: Value of the key.
        :returns: A ``bool`` to mark whether config is set or not.
        """
        return self.client.kv.put(self._merge_path(key), safe_value(value))

    def all(self) -> dict:
        """Get all key-value pairs.

        :returns: A ``dict`` of key-value pairs (if any).
        """
        _, resultset = self.client.kv.get(self._merge_path(""), recurse=True)

        if not resultset:
            return {}

        return {
            self._unmerge_path(item["Key"]): item["Value"].decode()
            for item in resultset
        }

    def _request_warning(self, scheme: str, verify: bool) -> None:
        """Emit warning about unverified request to unsecure Consul address.

        :params scheme: Scheme of Consul address.
        :params verify: Mark whether client needs to verify the address.
        """
        if scheme == "https" and verify is False:
            import urllib3

            urllib3.disable_warnings()
            logger.warning(
                "All requests to Consul will be unverified. "
                "Please adjust GLUU_CONFIG_CONSUL_SCHEME and "
                "GLUU_CONFIG_CONSUL_VERIFY environment variables."
            )

    def _token_from_file(self, path) -> str:
        """Get the token string from a path.

        :params path: Path to file contains token string.
        :returns: Token string.
        """
        if not os.path.isfile(path):
            return ""

        with open(path) as fr:
            token = fr.read().strip()
        return token

    def _verify_cert(self, scheme, verify, cacert_file, cert_file, key_file) -> Tuple[Union[None, tuple], Union[bool, str]]:
        """Verify client cert and key.

        :params scheme: Scheme of Consul address.
        :params verify: Mark whether client needs to verify the address.
        :params cacert_file: Path to CA cert file.
        :params cert_file: Path to client's cert file.
        :params key_file: Path to client's key file.
        :returns: A pair of cert key files (if exist) and verification.
        """
        cert = None

        if scheme == "https":
            verify = as_boolean(verify)

            # verify using CA cert (if any)
            if all([verify, os.path.isfile(cacert_file)]):
                verify = cacert_file

            if all([os.path.isfile(cert_file), os.path.isfile(key_file)]):
                cert = (cert_file, key_file)
        return cert, verify
