from core.utils.run_in_thread import to_thread
import io
from core.infra.blob.buckets import Buckets
from core.infra.blob.interface import AsyncBlobStorage
from core.infra.blob.registry import register_blob
from core.settings.config import StorageSettings
from minio import Minio

@register_blob("minio")
class MinioClient(AsyncBlobStorage):
    def __init__(self, cfg: StorageSettings):
        self._cli = Minio(
            cfg.endpoint,
            access_key=cfg.access_key,
            secret_key=cfg.secret_key,
            secure=cfg.secure,
        )

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def upload(self, bucket: Buckets, key: str, data: bytes, content_type: str="application/octet-stream") -> None:
        def _u() -> None:
            self._cli.put_object(
                bucket_name=bucket.value,
                object_name=key,
                data=io.BytesIO(data),
                length=len(data),
                content_type=content_type,
            )
        await to_thread(_u)

    async def get_object(self, bucket: Buckets, key: str) -> bytes:
        def _g() -> bytes:
            response = self._cli.get_object(bucket.value, key)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        return await to_thread(_g)

    async def get_presigned_url(self, bucket: Buckets, key: str, expires_seconds: int=3600) -> str:
        def _p() -> str:
            return self._cli.get_presigned_url(
                bucket_name=bucket.value,
                object_name=key,
                expires=expires_seconds,
            )
        return await to_thread(_p)