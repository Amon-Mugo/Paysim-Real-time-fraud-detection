# used in uploading the file into aws s3 ,it enables the use of loacl testing and also
# live production into aws s3 where critiacal outputs are stored


from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import boto3

from generator.csv_source import parse_s3_uri

logger = logging.getLogger(__name__)


def upload_file(path: Path, s3_uri: str, s3_client: Any | None = None) -> None:
    bucket, key = parse_s3_uri(s3_uri)
    if not path.is_file():
        raise FileNotFoundError(f"Nothing to upload, {path} does not exist")

    client = s3_client or boto3.client("s3")
    client.upload_file(str(path), bucket, key)
    logger.info("Uploaded %s to %s", path, s3_uri)
