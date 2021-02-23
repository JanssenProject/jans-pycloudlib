"""
jans.pycloudlib.config.google_config
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains config adapter class to interact with
Google Secret.
"""

from jans.pycloudlib.secret.google_secret import GoogleSecret
import sys
import logging
import os
import json
from typing import Any

from google.cloud import secretmanager
from google.api_core.exceptions import AlreadyExists, NotFound
from jans.pycloudlib.utils import safe_value

logger = logging.getLogger(__name__)


class GoogleConfig(GoogleSecret):
    """This class interacts with Google Secret backend.

    The following environment variables are used to instantiate the client:

    - ``GOOGLE_APPLICATION_CREDENTIALS`` json file that should be injected in upstream images
    - ``GOOGLE_PROJECT_ID``
    - ``CN_GOOGLE_SECRET_VERSION_ID``
    - ``CN_GOOGLE_SECRET_MANAGER_PASSPHRASE``
    - ``CN_SECRET_GOOGLE_SECRET``
    """

    def __init__(self, configuration=False):
        self.project_id = os.getenv("GOOGLE_PROJECT_ID")
        self.version_id = os.getenv("CN_GOOGLE_SECRET_VERSION_ID", "latest")
        # secrets key value by default
        self.google_secret_name = os.getenv("CN_SECRET_GOOGLE_SECRET", "jans") + "-configuration"
        # Create the Secret Manager client.
        self.client = secretmanager.SecretManagerServiceClient()

    def all(self) -> dict:
        """
        Access the payload for the given secret version if one exists. The version
        can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
        :returns: A ``dict`` of key-value pairs (if any)
        """
        # Build the resource name of the secret version.
        name = f"projects/{self.project_id}/secrets/{self.google_secret_name}/versions/{self.version_id}"
        data = {}
        retry = True
        while retry:
            try:
                # Access the secret version.
                response = self.client.access_secret_version(request={"name": name})
                logger.info(f"Secret {self.google_secret_name} has been found. Accessing version {self.version_id}.")
                payload = response.payload.data.decode("UTF-8")
                data = json.loads(payload)
                retry = False
            except NotFound:
                logger.warning("Secret may not exist or have any versions created")
                self.create_secret()
                self.add_secret_version(safe_value({}))

        return data

    def set(self, key: str, value: Any) -> bool:
        """Set key with given value.

        :params key: Key name.
        :params value: Value of the key.
        :params data full dictionary to push. Used in initial creation of config and secret
        :returns: A ``bool`` to mark whether config is set or not.
        """
        all = self.all()
        all[key] = safe_value(value)
        secret = self.create_secret()
        logger.info(f'Size of secret payload : {sys.getsizeof(safe_value(all))} bytes')
        secret_version_bool = self.add_secret_version(safe_value(all))
        return secret_version_bool

    def set_all(self, data: dict = None) -> bool:
        """Push a full dictionary to secrets.
        :params data full dictionary to push. Used in initial creation of config and secret
        :returns: A ``bool`` to mark whether config is set or not.
        """
        all = {}
        for k, v in data.items():
            all[k] = safe_value(v)
        secret = self.create_secret()
        logger.info(f'Size of secret payload : {sys.getsizeof(safe_value(all))} bytes')
        secret_version_bool = self.add_secret_version(safe_value(all))
        return secret_version_bool

    def create_secret(self) -> bool:
        """
        Create a new secret with the given name. A secret is a logical wrapper
        around a collection of secret versions. Secret versions hold the actual
        secret material.
        """

        # Build the resource name of the parent project.
        parent = f"projects/{self.project_id}"
        response = False
        try:
            # Create the secret.
            response = self.client.create_secret(
                request={
                    "parent": parent,
                    "secret_id": self.google_secret_name,
                    "secret": {"replication": {"automatic": {}}},
                }
            )
            logger.info("Created secret: {}".format(response.name))

        except AlreadyExists:
            logger.warning(f'Secret {self.google_secret_name} already exists. A new version will be created.')

        return bool(response)

    def add_secret_version(self, payload: str) -> bool:
        """
        Add a new secret version to the given secret with the provided payload.
        :params payload:  payload
        """

        # Build the resource name of the parent secret.
        parent = self.client.secret_path(self.project_id, self.google_secret_name)

        # Convert the string payload into a bytes. This step can be omitted if you
        # pass in bytes instead of a str for the payload argument.
        payload = payload.encode("UTF-8")

        # Add the secret version.
        response = self.client.add_secret_version(
            request={"parent": parent, "payload": {"data": payload}}
        )

        logger.info("Added secret version: {}".format(response.name))
        return bool(response)
