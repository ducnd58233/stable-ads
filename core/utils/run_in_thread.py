import anyio
import asyncio
import concurrent.futures
from typing import Callable, Coroutine, TypeVar

T = TypeVar("T")


async def to_thread(fn: Callable, *args, **kwargs):
    return await anyio.to_thread.run_sync(lambda: fn(*args, **kwargs))


def run_async_from_sync(coro: Coroutine[None, None, T]) -> T:
    """
    Run an async coroutine from a synchronous context.

    Handles both cases: when an event loop is already running (e.g., Airflow workers)
    and when no loop is running.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(asyncio.run, coro)
        return future.result()