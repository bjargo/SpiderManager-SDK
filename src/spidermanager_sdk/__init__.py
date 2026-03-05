"""
SpiderManager SDK — 极简爬虫数据上报库

典型用法::

    from spidermanager_sdk import sdk

    sdk.init()                               # 自动读取环境变量
    sdk.insert("articles", {"title": "..."}) # 内部缓冲, 达到阈值后异步上报
    sdk.flush()                              # 手动强制上报（一般不需要）
"""

from spidermanager_sdk.client import SpiderManagerClient
from spidermanager_sdk.aio import async_sdk, AsyncSpiderManagerClient

# ── 全局单例，用户直接操作此对象 ──
sdk: SpiderManagerClient = SpiderManagerClient()

__all__ = ["sdk", "SpiderManagerClient", "async_sdk", "AsyncSpiderManagerClient"]

try:
    from spidermanager_sdk._version import version as __version__
except ImportError:
    __version__ = "0.0.0"
