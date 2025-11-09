import asyncio
from ..bus.interfaces import AsyncPublisher, AsyncSubscriber, Envelope
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from core.settings.config import get_settings
from ..registry import register_publisher, register_subscriber

@register_publisher("kafka")
class KafkaAsyncPublisher(AsyncPublisher):
    def __init__(self):
        self._producer: AIOKafkaProducer | None = None
        self._broker = get_settings().mq.broker

    async def start(self):
        self._producer = AIOKafkaProducer(bootstrap_servers=self._broker)
        await self._producer.start()
    
    async def stop(self):
        if self._producer:
            await self._producer.stop()
            self._producer = None

    async def publish(self, envelope: Envelope) -> None:
        assert self._producer is not None
        headers = [(k, v.encode()) for k,v in envelope.headers.items()] if envelope.headers else None
        await self._producer.send_and_wait(envelope.topic, value=envelope.payload, key=envelope.key, headers=headers)


@register_subscriber("kafka")
class KafkaAsyncSubscriber(AsyncSubscriber):
    def __init__(self, group_id: str):
        s = get_settings().mq
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=s.broker,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
    
    async def start(self, topics: list[str]) -> None:
        assert self._consumer is not None
        await self._consumer.start()
    
    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
    
    async def poll(self, timeout_s: float = 1.0) -> Envelope | None:
        try:
            msg = await asyncio.wait_for(self._consumer.getone(), timeout=timeout_s)
        except asyncio.TimeoutError:
            return None
        headers = {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
                    for k, v in (msg.headers or []) }
        return Envelope(topic=msg.topic, key=msg.key, payload=msg.value, headers=headers, meta=msg)

    async def commit(self, env: Envelope) -> None:
        if env.meta is not None:
            await self._consumer.commit({env.meta.topic_partition: env.meta.offset + 1})
