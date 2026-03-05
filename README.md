# SpiderManager SDK

极简 Python SDK，将爬虫采集数据通过 HTTP 异步中转至 SpiderManager 后端，实现爬虫逻辑与数据存储的完全解耦。

## 特性

- **零配置启动**：自动从环境变量 `TASK_ID` / `SPIDER_API_URL` 读取配置
- **异步缓冲**：内存 buffer 按阈值（20条）或时间窗口（3秒）批量上报，减少网络 IO
- **容器安全**：通过 `atexit` + `SIGTERM` 双保险，确保 Docker 销毁前 flush 全部数据
- **最小依赖**：仅依赖 `httpx`，不侵入爬虫业务代码

## 安装

```bash
pip install -e .
```

## 快速开始

```python
from spidermanager_sdk import sdk

# 初始化（容器中自动读取环境变量，本地开发可手动指定）
sdk.init(api_url="http://localhost:8000", task_id="task-001")

# 上报数据（会自动缓冲、批量上报）
sdk.insert("articles", {"title": "Hello", "url": "https://example.com"})

# 批量上报
sdk.insert("articles", [
    {"title": "A", "url": "https://a.com"},
    {"title": "B", "url": "https://b.com"},
])

# 程序结束时自动 flush，也可手动触发
sdk.flush()
```

## 配置选项

| 参数 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `api_url` | `SPIDER_API_URL` | — | 后端地址 |
| `task_id` | `TASK_ID` | — | 任务 ID |
| `buffer_size` | — | `20` | 缓冲条数阈值 |
| `flush_interval` | — | `3.0` | 时间窗口（秒） |

## 架构

```
sdk.insert()
    ↓
FlushBuffer (内存缓冲, 线程安全)
    ↓  条数阈值 / 定时器触发
HttpTransport.send_batch()
    ↓
POST /api/v1/tasks/data/ingest?task_id=xxx
```
