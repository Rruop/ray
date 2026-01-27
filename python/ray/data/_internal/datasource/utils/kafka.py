""" Kafka related utility classes and functions."""

from dataclasses import dataclass, fields
from typing import Any, Optional, Dict


@dataclass
class KafkaAuthConfig:
    """Authentication configuration for Kafka connections.

    Uses standard kafka-python parameter names. See kafka-python documentation
    for full details: https://kafka-python.readthedocs.io/

    security_protocol: Protocol used to communicate with brokers.
        Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
        Default: PLAINTEXT.
    sasl_mechanism: Authentication mechanism when security_protocol
        is configured for SASL_PLAINTEXT or SASL_SSL. Valid values are:
        PLAIN, GSSAPI, OAUTHBEARER, SCRAM-SHA-256, SCRAM-SHA-512.
    sasl_plain_username: username for sasl PLAIN and SCRAM authentication.
        Required if sasl_mechanism is PLAIN or one of the SCRAM mechanisms.
    sasl_plain_password: password for sasl PLAIN and SCRAM authentication.
        Required if sasl_mechanism is PLAIN or one of the SCRAM mechanisms.
    sasl_kerberos_name: Constructed gssapi.Name for use with
        sasl mechanism handshake. If provided, sasl_kerberos_service_name and
        sasl_kerberos_domain name are ignored. Default: None.
    sasl_kerberos_service_name: Service name to include in GSSAPI
        sasl mechanism handshake. Default: 'kafka'
    sasl_kerberos_domain_name: kerberos domain name to use in GSSAPI
        sasl mechanism handshake. Default: one of bootstrap servers
    sasl_oauth_token_provider: OAuthBearer
        token provider instance. Default: None
    ssl_context: Pre-configured SSLContext for wrapping
        socket connections. If provided, all other ssl_* configurations
        will be ignored. Default: None.
    ssl_check_hostname: Flag to configure whether ssl handshake
        should verify that the certificate matches the brokers hostname.
        Default: True.
    ssl_cafile: Optional filename of ca file to use in certificate
        verification. Default: None.
    ssl_certfile: Optional filename of file in pem format containing
        the client certificate, as well as any ca certificates needed to
        establish the certificate's authenticity. Default: None.
    ssl_keyfile: Optional filename containing the client private key.
        Default: None.
    ssl_password: Optional password to be used when loading the
        certificate chain. Default: None.
    ssl_crlfile: Optional filename containing the CRL to check for
        certificate expiration. By default, no CRL check is done. When
        providing a file, only the leaf certificate will be checked against
        this CRL. The CRL can only be checked with Python 3.4+ or 2.7.9+.
        Default: None.
    ssl_ciphers: optionally set the available ciphers for ssl
        connections. It should be a string in the OpenSSL cipher list
        format. If no cipher can be selected (because compile-time options
        or other configuration forbids use of all the specified ciphers),
        an ssl.SSLError will be raised. See ssl.SSLContext.set_ciphers
    """

    # Security protocol
    security_protocol: Optional[str] = None

    # SASL configuration
    sasl_mechanism: Optional[str] = None
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None
    sasl_kerberos_name: Optional[str] = None
    sasl_kerberos_service_name: Optional[str] = None
    sasl_kerberos_domain_name: Optional[str] = None
    sasl_oauth_token_provider: Optional[Any] = None

    # SSL configuration
    ssl_context: Optional[Any] = None
    ssl_check_hostname: Optional[bool] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    ssl_password: Optional[str] = None
    ssl_ciphers: Optional[str] = None
    ssl_crlfile: Optional[str] = None

def add_authentication_to_config(
    config: Dict[str, Any], kafka_auth_config: Optional[KafkaAuthConfig]
) -> None:
    """Add authentication configuration to consumer config in-place.

    Args:
        config: Consumer config dict to modify.
        kafka_auth_config: Authentication configuration.
    """
    if kafka_auth_config:
        # Extract non-None fields from dataclass without copying objects
        for field in fields(kafka_auth_config):
            value = getattr(kafka_auth_config, field.name)
            if value is not None:
                config[field.name] = value