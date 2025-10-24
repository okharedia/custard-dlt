import io
from paramiko import RSAKey, Ed25519Key, ECDSAKey, DSSKey
from prefect import task
from prefect.cache_policies import INPUTS


@task(cache_policy=INPUTS)
def get_pkey_from_string(private_key_string: str):
    """Loads a private key from a string."""
    if not private_key_string:
        return None
    key_classes = [RSAKey, Ed25519Key, ECDSAKey, DSSKey]
    for key_class in key_classes:
        try:
            return key_class.from_private_key(io.StringIO(private_key_string))
        except Exception:
            continue
    return None
