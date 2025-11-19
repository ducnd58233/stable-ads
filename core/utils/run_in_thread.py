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
        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(_run_in_new_loop, coro)
            return future.result()
    except RuntimeError:
        try:
            return asyncio.run(coro)
        except Exception as e:
            raise e


def _run_in_new_loop(coro: Coroutine[None, None, T]) -> T:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    except Exception as e:
        raise e
    finally:
        try:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:
            pass
        finally:
            loop.close()
            asyncio.set_event_loop(None)