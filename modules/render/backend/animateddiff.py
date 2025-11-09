from .base import VideoBackend, register_backend
from .dto import BeatSpec, RenderRequest
from core.settings.config import DiffuserSettings
from diffusers import AnimateDiffPipeline, MotionAdapter, DDIMScheduler
import torch
from diffusers.utils import export_to_video
from diffusers.pipelines import DiffusionPipeline


@register_backend("animatediff")
class AnimateDiffBackend(VideoBackend):
    def __init__(self, cfg: DiffuserSettings) -> None:
        super().__init__(cfg)
        self._device = self._cfg.device
        self._dtype = torch.float16 if self._cfg.dtype == "float16" else torch.float32
        self._motion = None
        self._pipe = None

    def _offload(self, pipe: DiffusionPipeline) -> DiffusionPipeline:
        if self._cfg.cpu_offload:
            pipe.enable_model_cpu_offload()
        return pipe

    def _ad_pipe(self):
        if self._pipe is None:
            if self._device == "cuda":
                torch.cuda.empty_cache()
                torch.cuda.synchronize()
            
            motion_kwargs = {"torch_dtype": self._dtype} if self._device == "cuda" else {}
            pipe_kwargs = {"torch_dtype": self._dtype} if self._device == "cuda" else {}
            
            # Add cache directories to avoid re-downloading models
            motion_cache_dir = self._get_model_cache_dir("animatediff", self._cfg.ad_motion_adapter_id)
            base_cache_dir = self._get_model_cache_dir("animatediff", self._cfg.ad_base_model_id)
            
            motion_kwargs["cache_dir"] = motion_cache_dir
            pipe_kwargs["cache_dir"] = base_cache_dir
            
            self._motion = MotionAdapter.from_pretrained(
                self._cfg.ad_motion_adapter_id, 
                **motion_kwargs
            )
            pipe = AnimateDiffPipeline.from_pretrained(
                self._cfg.ad_base_model_id, 
                motion_adapter=self._motion, 
                **pipe_kwargs
            )
            pipe.scheduler = DDIMScheduler.from_config(pipe.scheduler.config)
            pipe = pipe.to(self._device)
            pipe = self._offload(pipe)
            self._pipe = pipe
        return self._pipe

    def _render_beat(self, title: str, style: str, beat: BeatSpec, fps: int):
        device_obj = torch.device(self._device)
        g = torch.Generator(device_obj).manual_seed(self._cfg.seed)
        prompt = f"{title}, {style}, {beat.scene}" + (f", {beat.broll_hint}" if beat.broll_hint else "")
        desired_frames = int(beat.seconds * fps)
        base_frames = max(8, min(self._cfg.ad_frames, desired_frames))
        res = self._ad_pipe()(
            prompt=prompt,
            num_inference_steps=self._cfg.ad_steps,
            guidance_scale=self._cfg.ad_guidance,
            num_frames=base_frames,
            generator=g
        )
        frames = res.frames[0] if isinstance(res.frames, list) else res.frames
        # After AnimateDiff, enforce exact frames = seconds * fps
        target = max(1, int(beat.seconds * fps))
        if len(frames) < target:
            frames += [frames[-1]] * (target - len(frames))
        elif len(frames) > target:
            frames = frames[:target]
        return frames

    def render(self, req: RenderRequest, out_path: str) -> str:
        title = req.title
        style = req.style or "neutral"
        all_frames = []
        for beat in req.beats:
            all_frames.extend(self._render_beat(title, style, beat, req.fps))
        export_to_video(all_frames, out_path, fps=req.fps)
        return out_path

    def close(self) -> None:
        import gc
        if self._pipe is not None: 
            self._pipe.to("cpu")
            del self._pipe
            self._pipe = None
        if self._motion is not None: 
            del self._motion
            self._motion = None
        gc.collect()
        if torch.cuda.is_available(): 
            torch.cuda.empty_cache()