import asyncio
import functools
import inspect
from typing import Callable, Set


def get_function_parameter_names(func: Callable) -> Set[str]:
    return set(inspect.signature(func).parameters.keys())


def async_wrap(func):
    if asyncio.iscoroutinefunction(func):
        return func

    @functools.wraps(func)
    async def run(*args, **kwargs):
        loop = asyncio.get_running_loop()
        pfunc = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, pfunc)

    return run
