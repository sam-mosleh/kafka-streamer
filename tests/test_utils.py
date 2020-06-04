import asyncio
import time

import pytest

from kafka_streamer.utils import async_wrap


@async_wrap
def sleep_sync_one_second():
    time.sleep(1)
    return True


@async_wrap
async def sleep_async_one_second():
    await asyncio.sleep(1)
    return True


async def run_async_ten_times():
    wrapped_syncs = [sleep_sync_one_second() for _ in range(5)]
    wrapped_asyncs = [sleep_async_one_second() for _ in range(5)]
    return await asyncio.gather(*(wrapped_syncs + wrapped_asyncs))


@pytest.mark.asyncio
async def test_async_wrap():
    start_time = time.time()
    all_results = await run_async_ten_times()
    end_time = time.time()
    elapsed = end_time - start_time
    assert len(all_results) == 10
    for result in all_results:
        assert result
    assert elapsed > 0.9
    assert elapsed < 1.1
