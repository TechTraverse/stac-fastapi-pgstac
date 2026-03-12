from typing import Optional

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Receive, Scope, Send


class ProxyHostMiddleware:
    def __init__(
        self, app: ASGIApp, proxy_scheme: Optional[str], proxy_host: Optional[str]
    ) -> None:
        self.app = app
        self.proxy_scheme = proxy_scheme
        self.proxy_host = proxy_host

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Override scope values if proxy paraamters are configured"""
        if scope["type"] == "http":
            if self.proxy_scheme:
                scope["scheme"] = self.proxy_scheme

            if self.proxy_host:
                scope["server"] = (self.proxy_host, 0)
                headers = MutableHeaders(scope=scope)
                headers["host"] = self.proxy_host

        return await self.app(scope, receive, send)
