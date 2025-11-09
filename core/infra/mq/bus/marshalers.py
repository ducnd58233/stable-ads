import json, uuid
from .registry import register_marshaler
from .interfaces import Marshaler, Envelope

@register_marshaler("json")
class JSONMarshaler(Marshaler):
    def __init__(self, version="v1"):
        self.version = version
    def dumps(self, obj, *, topic: str, headers=None) -> Envelope:
        hs = {"content-type":"application/json","schema-version":self.version,"message-id":str(uuid.uuid4())}
        if headers: hs.update(headers)
        return Envelope(topic=topic, key=None, payload=json.dumps(obj).encode(), headers=hs)
    def loads(self, env: Envelope):
        if env.headers.get("content-type")!="application/json": raise ValueError("unsupported content-type")
        return json.loads(env.payload.decode())