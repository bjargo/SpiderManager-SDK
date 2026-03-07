import socket
import logging
from urllib.parse import urlparse

logger = logging.getLogger("spidermanager_sdk.utils")

def resolve_provider_url(api_url: str) -> tuple[str, str | None]:
    """
    将 URL 中的域名解析为 IP，绕过 Docker DNS 抖动。
    返回: (基于IP的URL, 原始Hostname)
    """
    parsed = urlparse(api_url)
    hostname = parsed.hostname
    if not hostname:
        return api_url, None
    
    try:
        # 仅在初始化时执行一次同步解析
        ip = socket.gethostbyname(hostname)
        port = f":{parsed.port}" if parsed.port else ""
        # 重新构建 URL，保留协议和路径，替换域名为 IP
        resolved_url = f"{parsed.scheme}://{ip}{port}"
        return resolved_url, hostname
    except Exception as e:
        logger.warning(f"DNS 预解析失败: {e}，将回退到原始地址")
        return api_url, hostname