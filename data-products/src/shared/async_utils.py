import asyncio
from typing import Coroutine, Sequence

from prefect import task


async def process_parallel_subflows(
    coroutines: Sequence[Coroutine], concurrency_limit: int = 10
):
    """Helper function to submit async subflow with managed concurrency.

    Args:
        coroutines (Sequence[Coroutine]): List of coroutines
        concurrency_limit (int, optional): Explicit value to use. Defaults to 10.
    """
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def with_concurrency_limit(coroutine: Coroutine) -> Coroutine:
        async with semaphore:
            return await coroutine

    group = [with_concurrency_limit(coroutine) for coroutine in coroutines]

    await asyncio.gather(*group, return_exceptions=True)


@task()
async def process_parallel_subflows_task(
    coroutines: Sequence[Coroutine], concurrency_limit: int = 10
):
    """Task to manage subflow submission.

    Args:
        coroutines (Sequence[Coroutine]): List of coroutines
        concurrency_limit (int, optional): Explicit value to use. Defaults to 10.
    """
    await process_parallel_subflows(coroutines, concurrency_limit)
