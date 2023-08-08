import asyncio
from typing import Coroutine, Sequence


async def process_parallel_subflows(
    coroutines: Sequence[Coroutine], concurrency_limit: int = 10
):
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def with_concurrency_limit(coroutine: Coroutine) -> Coroutine:
        async with semaphore:
            return await coroutine

    group = [with_concurrency_limit(coroutine) for coroutine in coroutines]

    await asyncio.gather(*group, return_exceptions=True)
