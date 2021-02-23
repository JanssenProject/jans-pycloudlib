"""
jans.pycloudlib.secret.google_secret
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains secret adapter class to interact with
Kubernetes Secret.
"""

import hashlib
import sys
import os
import json
from binascii import hexlify, unhexlify
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.exceptions import InvalidTag
from typing import Any

from google.cloud import secretmanager
from google.api_core.exceptions import AlreadyExists, NotFound
from jans.pycloudlib.secret.base_secret import BaseSecret
from jans.pycloudlib.utils import safe_value
import lzma
import zlib


class GoogleSecret(BaseSecret):
    """This class interacts with Kubernetes Secret backend.

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
        self.salt = os.urandom(16)
        self.passphrase = os.getenv("CN_GOOGLE_SECRET_MANAGER_PASSPHRASE", "secret")
        # secrets key valye by default
        self.google_secret_name = os.getenv("CN_SECRET_GOOGLE_SECRET", "jans") + "-secret"
        if configuration:
            self.google_secret_name = os.getenv("CN_SECRET_GOOGLE_SECRET", "jans") + "-configuration"
        # Create the Secret Manager client.
        self.client = secretmanager.SecretManagerServiceClient()
        self.key = self._set_key()

    def _set_key(self) -> bytes:
        """
        Return key for for encrypting and decrypting payload
        :return: key
        """
        return hashlib.pbkdf2_hmac("sha256", self.passphrase.encode("utf8"), self.salt, 1000)

    def _encrypt(self, plaintext: str) -> str:
        """
        Encrypt payload
        :oarans plaintext: plain string to encrypt
        :return: A string including salr, iv, and encrypted payload
        """
        aes = AESGCM(self.key)
        iv = os.urandom(16)
        plaintext = plaintext.encode("utf8")
        plaintext = lzma.compress(plaintext)
        ciphertext = aes.encrypt(iv, plaintext, None)
        print(f'Size of encrypted secret payload : {sys.getsizeof(ciphertext)} bytes')
        return "%s-%s-%s" % (
            hexlify(self.salt).decode("utf8"), hexlify(iv).decode("utf8"), hexlify(ciphertext).decode("utf8"))

    def _decrypt(self, ciphertext: str) -> str:
        """
        Decrypt payload
        :params ciphertext: encrypted string to decrypt
        :return: decrypted payload
        """
        self.salt, iv, ciphertext = map(unhexlify, ciphertext.split("-"))
        self.key = self._set_key()
        aes = AESGCM(self.key)
        plaintext = ""
        try:
            plaintext = aes.decrypt(iv, ciphertext, None)
            plaintext = lzma.decompress(plaintext)
        except InvalidTag:
            print("Wrong passphrase used.")
        return plaintext.decode("utf8")

    def all(self) -> dict:
        """
        Access the payload for the given secret version if one exists. The version
        can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
        :returns: A ``dict`` of key-value pairs (if any)
        """
        # Build the resource name of the secret version.
        name = f"projects/{self.project_id}/secrets/{self.google_secret_name}/versions/{self.version_id}"
        data = {}
        retry = False
        while retry:
            try:
                # Access the secret version.
                response = self.client.access_secret_version(request={"name": name})
                print(f"Secret {self.google_secret_name} has been found. Accessing version {self.version_id}.")
                payload = zlib.decompress(response.payload.data).decode("UTF-8")
                data = json.loads(self._decrypt(payload))
                retry = False
            except NotFound:
                print("Secret may not exist or have any versions created")
                self.create_secret()
                self.add_secret_version(self._encrypt(safe_value({})))
                retry = True

        return data

    def get(self, key, default: Any = None) -> Any:
        """Get value based on given key.
        :params key: Key name.
        :params default: Default value if key is not exist.
        :returns: Value based on given key or default one.
        """
        result = self.all()
        return result.get(key) or default

    def set(self, key: str, value: Any, data: dict = None) -> bool:
        """Set key with given value.

        :params key: Key name.
        :params value: Value of the key.
        :params data full dictionary to push. Used in initial creation of config and secret
        :returns: A ``bool`` to mark whether config is set or not.
        """
        all = self.all()
        # Add the ability to inject the whole data dictionary at once. O
        # therwise, a new version will be created for each secret.
        if data:
            all = {}
            for k, v in data.items():
                all[k] = safe_value(v)
        else:
            all[key] = safe_value(value)
        secret = self.create_secret()
        print(f'Size of secret payload : {sys.getsizeof(safe_value(all))} bytes')
        secret_version_bool = self.add_secret_version(
            self._encrypt(safe_value(all)))
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
            # Print the new secret name.
            print("Created secret: {}".format(response.name))

        except AlreadyExists:
            print(f'Secret {self.google_secret_name} already exists. A new version will be created.')

        return bool(response)

    def add_secret_version(self, payload: str) -> bool:
        """
        Add a new secret version to the given secret with the provided payload.
        :params payload: encrypted payload
        """

        # Build the resource name of the parent secret.
        parent = self.client.secret_path(self.project_id, self.google_secret_name)

        # Convert the string payload into a bytes. This step can be omitted if you
        # pass in bytes instead of a str for the payload argument.
        payload = zlib.compress(payload.encode("UTF-8"))

        # Add the secret version.
        response = self.client.add_secret_version(
            request={"parent": parent, "payload": {"data": payload}}
        )

        # Print the new secret version name.
        print("Added secret version: {}".format(response.name))
        return bool(response)

    def delete(self) -> None:
        """
        Delete the secret with the given name and all of its versions.
        """
        # Build the resource name of the secret.
        name = self.client.secret_path(self.project_id, self.google_secret_name)

        try:
            # Delete the secret.
            self.client.delete_secret(request={"name": name})
        except NotFound:
            print(f'Secret {self.google_secret_name} does not exist in the secret manager.')
