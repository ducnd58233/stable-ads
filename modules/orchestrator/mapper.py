from .dto import ScriptSchema
from modules.render import BeatSpec, RenderRequest

def to_render_request(script: ScriptSchema, fps: int) -> RenderRequest:
    beats = [
        BeatSpec(
            scene=b.scene, 
            narration=b.narration, 
            broll_hint=b.broll_hint, 
            seconds=b.seconds,
        ) for b in script.beats
    ]
    return RenderRequest(
        title=script.title, 
        style=script.style, 
        beats=beats, 
        fps=fps
    )