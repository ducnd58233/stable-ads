from typing import Awaitable, Callable
from .interfaces import AsyncHandler, AsyncMiddleware, AsyncPublisher, AsyncSubscriber, Envelope, Marshaler


class AsyncRouter:
    def __init__(self, sub: AsyncSubscriber, pub: AsyncPublisher):
        self._sub = sub
        self._routes: dict[str, tuple[Marshaler, AsyncHandler, list[AsyncMiddleware]]] = {}
        self._running = False
    
    def handle(
        self, 
        topic: str, 
        marshaler: Marshaler, 
        handler: AsyncHandler, 
        middlewares: list[AsyncMiddleware] | None=None
    ) -> None:
        self._routes[topic] = (marshaler, handler, middlewares or [])

    def _chain(self, base: Callable[[Envelope], Awaitable[None]], mws: list[AsyncMiddleware]):
        fn = base
        for mw in reversed(mws):
            fn = mw(fn)
        return fn

    async def start(self):
        await self._sub.start(list(self._routes.keys()))
        self._running = True

        try:
            while self._running:
                env = await self._sub.poll(1.0)
                if not env: continue
                if env.topic not in self._routes: continue
                marshaler, handler, mws = self._routes[env.topic]
                async def base(env: Envelope):
                    msg = marshaler.loads(env)
                    await handler(msg)
                wrapped = self._chain(base, mws)
                try:
                    await wrapped(env)
                    await self._sub.commit(env)
                except Exception as e:
                    await self._sub.nack(env)
        except Exception as e:
            self._running = False
        finally:
            await self._sub.stop()

def mw_retry_dlq(pub: AsyncPublisher, dlq_topic: str, max_attempts: int=3):
    def _mw(next_fn):
        async def _inner(env: Envelope):
            attempt = int(env.headers.get("x-attempt","1"))
            try:
                return await next_fn(env)
            except Exception as e:
                if attempt >= max_attempts:
                    env.headers = dict(env.headers); env.headers["x-error"]=str(e)
                    env.topic = dlq_topic
                    await pub.publish(env); return
                env.headers = dict(env.headers); env.headers["x-attempt"]=str(attempt+1)
                await pub.publish(env)
        return _inner
    return _mw
