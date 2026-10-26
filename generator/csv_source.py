# used in opening paysim csv from its local path or in s3 and uses the s3 objects are
# structured so that the file can be never fully downloaded or held in memory

from __future__ import annotations
from typing import (
    TextIO,
    Any,
)  # textio used in streams of the data and any is used in for any text type
from collections.abc import Iterator  # generate return types
from contextlib import (
    contextmanager,
)  # used for callable functions using with statement
from generator.config import GeneratorConfig, S3_URI_PREFIX
import io  # used wraping binary streams into text streams
import boto3  # used in interacting with amazon aws


# used in validating the uri path and spliting it inot bucket and file path due
# to boto3 requires it in AWS
def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith(S3_URI_PREFIX):
        raise ValueError(f"S3 URI must start with {S3_URI_PREFIX}")
    bucket, _, key = uri[len(S3_URI_PREFIX) :].partition("/")
    # bucket becomes my-bucket name
    # key becomes data/file.csv
    # _ becomes "/"

    if not bucket or not key:
        raise ValueError("S3 URI must look like s3://<bucket>/<key>")

    return bucket, key


# used as a flexible contect manager file that opens csv file as text streams or if it is in s3 aws
@contextmanager
def open_csv(
    settings: GeneratorConfig, s3_client: Any | None = None
) -> Iterator[TextIO]:
    if settings.csv_s3_uri:
        bucket, key = parse_s3_uri(settings.csv_s3_uri)
        client = s3_client or boto3.client("s3")
        body = client.get_object(Bucket=bucket, Key=key)["Body"]
        stream: TextIO = io.TextIOWrapper(body, encoding="utf-8", newline="")

    else:  # if the csv leave in the machine
        stream = settings.csv_path.open(newline="", encoding="utf-8")

    with stream:
        yield stream
