from minio import Minio

minio_endpoint = "minio:9000"
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"


def create_minio_client():
    client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,  # True for https
    )

    return client
