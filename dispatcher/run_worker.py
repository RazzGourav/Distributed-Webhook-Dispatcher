import asyncio

from dispatcher.config import Settings
from dispatcher.runtime import runtime


async def main():
    async with runtime(Settings(embedded_worker=True)) as state:
        await state["worker"]


if __name__ == "__main__":
    asyncio.run(main())
