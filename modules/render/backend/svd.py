import numpy as np
import torch
from PIL import Image
from .base import VideoBackend, register_backend
from .dto import BeatSpec, RenderRequest
from core.settings.config import DiffuserSettings
from diffusers import StableDiffusionPipeline, StableVideoDiffusionPipeline
from diffusers.pipelines import DiffusionPipeline
from diffusers.utils import export_to_video

@register_backend("svd")
class SVDBackend(VideoBackend):
    def __init__(self, cfg: DiffuserSettings):
        super().__init__(cfg)
        self._device = self._cfg.device
        self._dtype = torch.float16 if self._cfg.dtype == "float16" else torch.float32

        self._svd: StableVideoDiffusionPipeline | None = None
        self._text_to_image: StableDiffusionPipeline | None = None

    def _offload(self, pipe: DiffusionPipeline) -> DiffusionPipeline:
        if self._cfg.cpu_offload:
            pipe.enable_model_cpu_offload()
        return pipe

    def _text_to_image_pipe(self) -> StableDiffusionPipeline:
        if self._text_to_image is None:
            if self._device == "cuda":
                torch.cuda.empty_cache()
                torch.cuda.synchronize()
            
            pipe_kwargs = {"torch_dtype": self._dtype} if self._device == "cuda" else {}
            
            cache_dir = self._get_model_cache_dir("text-to-image", self._cfg.sdxl_model_id)
            pipe_kwargs["cache_dir"] = cache_dir
            
            self._text_to_image = StableDiffusionPipeline.from_pretrained(
                self._cfg.sdxl_model_id,
                **pipe_kwargs
            )
            self._text_to_image = self._text_to_image.to(self._device)
            self._text_to_image = self._offload(self._text_to_image)
        return self._text_to_image

    def _svd_pipe(self) -> StableVideoDiffusionPipeline:
        if self._svd is None:
            if self._device == "cuda":
                torch.cuda.empty_cache()
                torch.cuda.synchronize()
            
            pipe_kwargs = {"torch_dtype": self._dtype} if self._device == "cuda" else {}
            
            cache_dir = self._get_model_cache_dir("svd", self._cfg.svd_model_id)
            pipe_kwargs["cache_dir"] = cache_dir
            
            self._svd = StableVideoDiffusionPipeline.from_pretrained(
                self._cfg.svd_model_id,
                **pipe_kwargs
            )
            self._svd = self._svd.to(self._device)
            self._svd = self._offload(self._svd)
        return self._svd

    def _keyframe(self, s: str, style: str, beat: BeatSpec) -> Image.Image:
        device_obj = torch.device(self._device)
        g = torch.Generator(device_obj).manual_seed(self._cfg.seed)
        prompt = f"{s}, {style}, {beat.scene}"
        return self._text_to_image_pipe()(
            prompt=prompt, 
            width=self._cfg.width, 
            height=self._cfg.height,
            num_inference_steps=20,
            generator=g
        ).images[0]

    def _animate(self, keyframe: Image.Image, frames_target: int):
        """
        SVD returns a clip of N frames based on parameters — we approximate duration by
        repeating or trimming to exactly frames_target to honor beat.seconds × fps.
        """
        device_obj = torch.device(self._device)
        g = torch.Generator(device_obj).manual_seed(self._cfg.seed)
        res = self._svd_pipe()(
            keyframe,
            decode_chunk_size=self._cfg.svd_decode_chunk,
            num_frames=max(8, min(48, frames_target)),
            num_inference_steps=self._cfg.svd_num_steps,
            motion_bucket_id=self._cfg.svd_motion_bucket,
            generator=g
        )
        frames = res.frames[0] if isinstance(res.frames, list) else res.frames
        # Adjust length to frames_target
        if len(frames) < frames_target:
            # loop last frame to pad
            tail = [frames[-1]] * (frames_target - len(frames))
            frames = frames + tail
        elif len(frames) > frames_target:
            frames = frames[:frames_target]
        return frames

    def render(self, req: RenderRequest, out_path: str) -> str:
        title = req.title
        style = req.style or "neutral"
        all_frames = []
        for beat in req.beats:
            target = max(1, int(beat.seconds * req.fps))
            kf = self._keyframe(title, style, beat)
            
            if self._device == "cuda":
                torch.cuda.empty_cache()
            
            frames = self._animate(kf, frames_target=target)
            all_frames.extend(frames)
        export_to_video(all_frames, out_path, fps=req.fps)
        return out_path

    def close(self) -> None:
        import gc
        if self._text_to_image is not None: 
            self._text_to_image.to("cpu")
            del self._text_to_image
            self._text_to_image = None
        if self._svd is not None:  
            self._svd.to("cpu")
            del self._svd
            self._svd = None
        gc.collect()
        if torch.cuda.is_available(): 
            torch.cuda.empty_cache()

