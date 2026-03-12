import json
from typing import Optional

from fastapi.responses import JSONResponse
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

        if request.url.path == "/" and response.status_code == 200:
            body = b"".join([chunk async for chunk in response.body_iterator])
            landing_page = json.loads(body)

            for link in landing_page.get("links", []):
                if link.get("rel") == "service-desc":
                    link["href"] = f"{self.settings.prefix_path}{request.app.openapi_url}"
                if link["rel"] == "service-doc":
                    link["href"] = f"{self.settings.prefix_path}{request.app.docs_url}"

            return JSONResponse(content=landing_page)

        return response
