import anyio
from typing import Callable

async def to_thread(fn: Callable, *args, **kwargs):
    return await anyio.to_thread.run_sync(lambda: fn(*args, **kwargs))