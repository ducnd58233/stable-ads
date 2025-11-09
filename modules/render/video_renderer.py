from .backend.base import get_backend
from .backend.dto import RenderRequest
import tempfile
from pathlib import Path

class VideoRenderer:
    def __init__(self):
        self._backend = get_backend()

    def render(self, req: RenderRequest) -> bytes:
        with tempfile.TemporaryDirectory() as tmpd:
            out_path = str(Path(tmpd) / "out.mp4")
            self._backend.render(req, out_path)
            return Path(out_path).read_bytes()

    def close(self) -> None:
        self._backend.close()
