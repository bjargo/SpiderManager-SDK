"""
HTTP 传输层

负责将序列化后的数据批量 POST 到 SpiderManager 后端。
使用 httpx 同步客户端，确保在 atexit / 信号处理等
非 async 上下文中也能可靠发送。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import httpx

logger = logging.getLogger("spidermanager_sdk.transport")

# 上报接口固定路径
_INGEST_PATH: str = "/api/v1/tasks/data/ingest"

# 默认超时配置（连接 / 读取 / 写入 / 总计）
_DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=30.0)


@dataclass
class HttpTransport:
    """
    同步 HTTP 传输客户端。

    Parameters
    ----------
    api_url : str
        SpiderManager 后端根地址，例如 ``http://backend:8000``。
    task_id : str
        当前任务 ID，每次请求作为 query 参数传递。
    """
    api_url: str = ""
    task_id: str = ""

    _client: httpx.Client | None = field(default=None, init=False, repr=False)

    # ──────────────────────────────────────────────
    # 生命周期
    # ──────────────────────────────────────────────

    def open(self) -> None:
        """初始化底层 httpx 连接池。"""
        if self._client is not None:
            return
        self._client = httpx.Client(
            base_url=self.api_url,
            timeout=_DEFAULT_TIMEOUT,
            headers={"Content-Type": "application/json"},
        )
        logger.debug("HTTP transport opened → %s", self.api_url)

    def close(self) -> None:
        """关闭连接池，释放资源。"""
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                logger.debug("关闭 HTTP 客户端时出现异常", exc_info=True)
            finally:
                self._client = None

    # ──────────────────────────────────────────────
    # 数据上报
    # ──────────────────────────────────────────────

    def send_batch(self, table_name: str, records: list[dict[str, Any]]) -> bool:
        """
        向后端发送一批数据。

        Parameters
        ----------
        table_name : str
            目标表名。
        records : list[dict[str, Any]]
            数据记录列表。

        Returns
        -------
        bool
            上报成功返回 True，否则 False（不抛异常，由调用方决策）。
        """
        if not self._client:
            self.open()
            assert self._client is not None

        url = _INGEST_PATH
        payload: dict[str, Any] = {
            "table_name": table_name,
            "data": records,
        }
        params: dict[str, str] = {"task_id": self.task_id}

        try:
            response = self._client.post(url, json=payload, params=params)
            if response.status_code == 200:
                logger.debug(
                    "上报成功: table=%s, count=%d", table_name, len(records),
                )
                return True
            else:
                logger.warning(
                    "上报失败 HTTP %d: %s", response.status_code, response.text[:200],
                )
                return False
        except httpx.TimeoutException:
            logger.error("上报超时: table=%s, count=%d", table_name, len(records))
            return False
        except httpx.ConnectError as exc:
            logger.error("连接后端失败 (%s): %s", self.api_url, exc)
            return False
        except Exception:
            logger.exception("上报数据时发生未知异常")
            return False
