"""
SpiderManager SDK 异步客户端 (asyncio)

提供原生的 async/await 支持，适用于各类异步爬虫框架 (如 httpx, aiohttp, playwright)。
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

import httpx

from spidermanager_sdk.buffer import BufferEntry
from spidermanager_sdk.client import _DEFAULT_BUFFER_SIZE, _DEFAULT_FLUSH_INTERVAL
from spidermanager_sdk.transport import _INGEST_PATH, _DEFAULT_TIMEOUT

logger = logging.getLogger("spidermanager_sdk.aio")

@dataclass
class AsyncHttpTransport:
    api_url: str = ""
    task_id: str = ""
    _client: httpx.AsyncClient | None = field(default=None, init=False, repr=False)

    async def open(self) -> None:
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(
            base_url=self.api_url,
            timeout=_DEFAULT_TIMEOUT,
            headers={"Content-Type": "application/json"},
        )

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass
            finally:
                self._client = None

    async def send_batch(self, table_name: str, records: list[dict[str, Any]]) -> bool:
        if not self._client:
            await self.open()
            assert self._client is not None

        url = _INGEST_PATH
        payload: dict[str, Any] = {"table_name": table_name, "data": records}
        params: dict[str, str] = {"task_id": self.task_id}

        try:
            response = await self._client.post(url, json=payload, params=params)
            if response.status_code == 200:
                return True
            logger.warning("上报失败 HTTP %d", response.status_code)
            return False
        except Exception as exc:
            logger.error("上报异常: %s", exc)
            return False

@dataclass
class AsyncFlushBuffer:
    max_size: int = 20
    flush_interval: float = 3.0
    on_flush: Callable[[list[BufferEntry]], Any] | None = None

    _entries: list[BufferEntry] = field(default_factory=list, init=False, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _task: asyncio.Task[None] | None = field(default=None, init=False, repr=False)
    _started: bool = field(default=False, init=False, repr=False)

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._started = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self.flush()

    async def add(self, entry: BufferEntry) -> None:
        async with self._lock:
            self._entries.append(entry)
            current_size = len(self._entries)

        if current_size >= self.max_size:
            await self.flush()

    async def flush(self) -> None:
        async with self._lock:
            if not self._entries:
                return
            batch = self._entries.copy()
            self._entries.clear()

        if self.on_flush:
            try:
                res = self.on_flush(batch)
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                logger.exception("flush 处理失败")

    async def _loop(self) -> None:
        while self._started:
            try:
                await asyncio.sleep(self.flush_interval)
                await self.flush()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("异步缓冲区定时器异常")

class AsyncSpiderManagerClient:
    def __init__(self) -> None:
        self._api_url: str = ""
        self._task_id: str = ""
        self._initialized: bool = False
        self._transport: AsyncHttpTransport | None = None
        self._buffer: AsyncFlushBuffer | None = None

    async def init(
        self,
        api_url: str | None = None,
        task_id: str | None = None,
        *,
        buffer_size: int = _DEFAULT_BUFFER_SIZE,
        flush_interval: float = _DEFAULT_FLUSH_INTERVAL,
    ) -> None:
        self._api_url = api_url or os.environ.get("SPIDER_API_URL", "")
        self._task_id = task_id or os.environ.get("TASK_ID", "")
        if not self._api_url or not self._task_id:
            raise ValueError("api_url 或 task_id 未配置 (或环境变量缺失)")
        self._api_url = self._api_url.rstrip("/")

        self._transport = AsyncHttpTransport(api_url=self._api_url, task_id=self._task_id)
        await self._transport.open()

        self._buffer = AsyncFlushBuffer(
            max_size=buffer_size,
            flush_interval=flush_interval,
            on_flush=self._handle_flush,
        )
        await self._buffer.start()
        self._initialized = True
        logger.info("Async SDK 初始化: url=%s task=%s", self._api_url, self._task_id)

    async def insert(self, table_name: str, data: dict[str, Any] | list[dict[str, Any]]) -> None:
        self._ensure_initialized()
        if isinstance(data, dict):
            data = [data]
        if not data:
            return
        assert self._buffer is not None
        for record in data:
            await self._buffer.add(BufferEntry(table_name=table_name, data=record))

    async def flush(self) -> None:
        if self._buffer:
            await self._buffer.flush()

    async def shutdown(self) -> None:
        if self._buffer:
            await self._buffer.stop()
        if self._transport:
            await self._transport.close()
        self._initialized = False

    async def __aenter__(self):
        # 兼容自动初始化
        if not self._initialized:
            await self.init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._initialized:
            await self.shutdown()

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError("SDK 尚未初始化")

    async def _handle_flush(self, entries: list[BufferEntry]) -> None:
        if not self._transport:
            return
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for entry in entries:
            grouped[entry.table_name].append(entry.data)
        
        # 内部并发上报多表
        tasks = []
        for table_name, records in grouped.items():
            tasks.append(self._transport.send_batch(table_name, records))
        
        if tasks:
            await asyncio.gather(*tasks)

# 默认全局异步单例
async_sdk = AsyncSpiderManagerClient()
