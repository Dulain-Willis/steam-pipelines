from io import BytesIO

from pipelines.common.storage.minio_client import create_minio_client


def upload_to_minio(
    bucket: str,
    object_name: str,
    raw_bytes: bytes,
    content_type: str,
):
    client = create_minio_client()

    stream = BytesIO(raw_bytes)

    client.put_object(
        bucket,
        object_name,
        stream,
        length=len(raw_bytes),
        content_type=content_type,
    )
