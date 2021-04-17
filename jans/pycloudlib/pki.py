"""
jans.pycloudlib.pki
~~~~~~~~~~~~~~~~~~~

This module contains various Public Key Infrastucture (PKI) helpers.
"""

import os
from datetime import datetime
from datetime import timedelta
from ipaddress import IPv4Address

from cryptography.hazmat.backends import default_backend
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


class PkiHelper:
    """This class provides various methods to generate cert, key, and cert
    signing request commonly used in Public Key Infrastructure (PKI).
    """

    def __init__(self):
        self.backend = default_backend()

    def generate_private_key(self, filename, passphrase=""):
        """Generate private key.

        :param filename: Path to generate key.
        :param passphrase: Passphrase for private key.
        """

        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=self.backend,
        )

        if passphrase:
            alg = serialization.BestAvailableEncryption(passphrase.encode())
        else:
            alg = serialization.NoEncryption()

        # write key into a file
        with open(filename, "wb") as f:
            f.write(private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=alg,
            ))
        return private_key

    def generate_public_key(self, filename, private_key, email, hostname,
                            org_name, country_code, state, city):
        """Generate public key (cert).

        :param filename: Path to generate key.
        :param private_key: An instance of PrivateKey object.
        :param email: Email address for subject/issuer.
        :param hostname: Hostname (common name) for subject/issuer.
        :param org_name: Organization name for subject/issuer.
        :param country_code: Country name in ISO format for subject/issuer.
        :param state: State/province name for subject/issuer.
        :param city: City/locality name for subject/issuer.
        """

        valid_from = datetime.utcnow()
        valid_to = valid_from + timedelta(days=365)

        # issuer equals subject because we use self-signed
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, country_code),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, state),
            x509.NameAttribute(NameOID.LOCALITY_NAME, city),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, org_name),
            x509.NameAttribute(NameOID.COMMON_NAME, hostname),
            x509.NameAttribute(NameOID.EMAIL_ADDRESS, email),
        ])

        builder = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(private_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(valid_from)
            .not_valid_after(valid_to)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        )

        public_key = builder.sign(
            private_key, hashes.SHA256(), backend=self.backend,
        )

        # write key into a file
        with open(filename, "wb") as f:
            f.write(public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
            ))
        return public_key

    def generate_csr(self, filename, private_key, email, hostname, org_name,
                     country_code, state, city, extra_dns=None, extra_ips=None):
        """Generate certificate signing request (CSR).

        :param filename: Path to generate CSR.
        :param private_key: An instance of PrivateKey object.
        :param email: Email address for subject/issuer.
        :param hostname: Hostname (common name) for subject/issuer.
        :param org_name: Organization name for subject/issuer.
        :param country_code: Country name in ISO format for subject/issuer.
        :param state: State/province name for subject/issuer.
        :param city: City/locality name for subject/issuer.
        :param extra_dns: A list of additional domain names for SubjectAlternativeName.
        :param extra_ips: A list of additional IP addresses for SubjectAlternativeName.
        """

        subject = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, country_code),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, state),
            x509.NameAttribute(NameOID.LOCALITY_NAME, city),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, org_name),
            x509.NameAttribute(NameOID.COMMON_NAME, hostname),
            x509.NameAttribute(NameOID.EMAIL_ADDRESS, email),
        ])

        # SANs
        extra_dns = extra_dns or []
        extra_ips = extra_ips or []

        # get basename without extension
        suffix, _ = os.path.splitext(os.path.basename(filename))

        sans = [
            x509.DNSName(hostname),
            x509.DNSName(suffix),
        ]

        # add Domains to SAN
        for dn in extra_dns:
            sans.append(x509.DNSName(dn))

        # add IPs to SAN
        for ip in extra_ips:
            sans.append(x509.IPAddress(IPv4Address(ip)))

        # make SANs unique
        sans = list(set(sans))

        builder = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(subject)
            .add_extension(x509.SubjectAlternativeName(sans), critical=False)
        )

        csr = builder.sign(private_key, hashes.SHA256(), backend=self.backend)

        with open(filename, "wb") as f:
            f.write(csr.public_bytes(
                serialization.Encoding.PEM
            ))
        return csr

    def sign_csr(self, filename, csr, ca_private_key, ca_public_key):
        """Sign a Certificate Signing Request (CSR).

        :param filename: Path to generate certificate.
        :param csr: An instance of CertificateSigningRequest object.
        :param ca_private_key: An instance of CA PrivateKey object.
        :param ca_public_key: An instance of CA Certificate object.
        """

        valid_from = datetime.utcnow()
        valid_to = valid_from + timedelta(days=365)

        builder = (
            x509.CertificateBuilder()
            .subject_name(csr.subject)
            .issuer_name(ca_public_key.subject)
            .public_key(csr.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(valid_from)
            .not_valid_after(valid_to)
        )

        for ext in csr.extensions:
            builder = builder.add_extension(ext.value, ext.critical)

        public_key = builder.sign(
            ca_private_key, hashes.SHA256(), backend=self.backend,
        )

        with open(filename, "wb") as f:
            f.write(public_key.public_bytes(
                serialization.Encoding.PEM
            ))
        return public_key
