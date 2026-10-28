# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""asyncio client: ``from ruvio.asyncio import Ruvio``.

Exports :class:`Ruvio`, :class:`ClusterRuvio`, :class:`ReadPolicy`, :class:`Pipeline`, :class:`PubSub` and :class:`Lock`. Every
command method returns an awaitable (``await client.get("k")``); one client is safe to share between
coroutines.

Example:
    import asyncio
    from ruvio.asyncio import Ruvio

    async def main():
        async with Ruvio("127.0.0.1", 6379) as client:
            await client.set("k", "v")
            print(await client.get("k"))

    asyncio.run(main())
"""

from .._cluster import ReadPolicy
from .client import Lock, Pipeline, PubSub, Ruvio
from .cluster import ClusterRuvio

__all__ = ["Ruvio", "ClusterRuvio", "ReadPolicy", "Pipeline", "PubSub", "Lock"]
