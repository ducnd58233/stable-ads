from .backend.base import get_backend
from .backend.dto import RenderRequest
import os

class VideoRenderer:
    def __init__(self):
        self._backend = get_backend()
        self._prefix = "generated/videos/"

    def render(self, req: RenderRequest) -> str:
        name = req.title.replace(" ", "_")
        out = os.path.join(self._prefix, f"{name}.mp4")
        self._backend.render(req, out)
        return out

    def close(self) -> None:
        self._backend.close()
