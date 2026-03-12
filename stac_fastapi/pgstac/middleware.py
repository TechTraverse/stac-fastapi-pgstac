import json
from typing import Optional

from starlette.datastructures import MutableHeaders
from starlette.middleware.base import BaseHTTPMiddleware
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


class FixAPILinksMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, settings):
        super().__init__(app)
        self.settings = settings

    async def dispatch(self, request, call_next):
        response = await call_next(request)

        if request.url.path == "/":
            data = json.loads(response.body)

            for link in data.get("links", []):
                if link["rel"] == "service-desc":
                    link["href"] = f"{self.settings.prefix_path}{request.app.openapi_url}"
                if link["rel"] == "service-doc":
                    link["href"] = f"{self.settings.prefix_path}{request.app.docs_url}"

            response.body = json.dumps(data).encode()

        return response
