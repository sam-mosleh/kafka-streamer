import asyncio
import functools
import inspect
from typing import Callable

# def raise_if_function_has_multiple_parameters(func: Callable):
#     params = inspect.signature(func).parameters
#     if len(params) > 1:
#         raise TypeError(
#             f"Function {func.__name__} must have only one parameter.")

# def get_first_parameter_type_of_function(func: Callable):
#     params = inspect.signature(func).parameters
#     first_param_annotation = params[next(iter(params))].annotation
#     if first_param_annotation == inspect._empty:
#         first_param_annotation = None
#     return first_param_annotation


def get_function_parameter_names(func: Callable):
    return tuple(inspect.signature(func).parameters.keys())


def async_wrap(func):
    if asyncio.iscoroutinefunction(func):
        return func

    @functools.wraps(func)
    async def run(*args, **kwargs):
        loop = asyncio.get_running_loop()
        pfunc = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, pfunc)

    return run
