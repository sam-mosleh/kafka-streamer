import asyncio
import functools
import inspect
from typing import Callable


async def call_sync_function_without_none_parameter(function: Callable,
                                                    **kwargs):
    loop = asyncio.get_running_loop()
    partial = functools.partial(function, **none_value_removed_dict(kwargs))
    return await loop.run_in_executor(None, partial)


def none_value_removed_dict(d: dict):
    return {key: value for key, value in d.items() if value is not None}


def raise_if_function_has_multiple_parameters(func: Callable):
    params = inspect.signature(func).parameters
    if len(params) > 1:
        raise TypeError(
            f"Function {func.__name__} must have only one parameter.")


def get_first_parameter_type_of_function(func: Callable):
    params = inspect.signature(func).parameters
    first_param_annotation = params[next(iter(params))].annotation
    if first_param_annotation == inspect._empty:
        first_param_annotation = None
    return first_param_annotation


def async_wrap(func):
    if asyncio.iscoroutinefunction(func):
        return func

    @functools.wraps(func)
    async def run(*args, **kwargs):
        loop = asyncio.get_running_loop()
        pfunc = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, pfunc)

    return run
