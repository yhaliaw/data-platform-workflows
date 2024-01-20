import dataclasses
import logging
import os
import subprocess

import boto3
import pytest


@dataclasses.dataclass(frozen=True)
class ConnectionInformation:
    access_key_id: str
    secret_access_key: str
    bucket: str


@pytest.fixture(scope="session")
def microceph():
    if "microceph" in subprocess.check_output(
        ["snap", "list"]
    ).decode("utf-8"):
        logger.info("Microceph already installed, keeping it")
        return ConnectionInformation(_ACCESS_KEY, _SECRET_KEY, _BUCKET)

    logger.info("Setting up microceph")
    subprocess.run(["sudo", "snap", "install", "microceph"], check=True)
    subprocess.run(["sudo", "microceph", "cluster", "bootstrap"], check=True)
    subprocess.run(
        ["sudo", "microceph", "disk", "add", "loop,4G,3"],
        check=True
    )
    subprocess.run(["sudo", "microceph", "enable", "rgw"], check=True)
    subprocess.run(
        [
            "sudo",
            "microceph.radosgw-admin",
            "user",
            "create",
            "--uid",
            "test",
            "--display-name",
            "test",
        ],
        capture_output=True,
        check=True,
        encoding="utf-8",
    ).stdout
    logger.info("Creating keys")
    subprocess.run(
        [
            "sudo",
            "microceph.radosgw-admin",
            "key",
            "create",
            "--uid=test",
            "--key-type=s3",
            "--access-key", _ACCESS_KEY,
            "--secret-key", _SECRET_KEY,
        ],
        capture_output=True,
        check=True,
        encoding="utf-8",
    ).stdout
    logger.info("Creating microceph bucket")
    boto3.client(
        "s3",
        endpoint_url="http://localhost",
        aws_access_key_id=_ACCESS_KEY,
        aws_secret_access_key=_SECRET_KEY,
    ).create_bucket(Bucket=_BUCKET)
    logger.info("Set up microceph")
    return ConnectionInformation(_ACCESS_KEY, _SECRET_KEY, _BUCKET)


_ACCESS_KEY = "access-key"
_SECRET_KEY = "secret-key"
_BUCKET = "testbucket"
logger = logging.getLogger(__name__)
