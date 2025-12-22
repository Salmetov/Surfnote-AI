import asyncio
import json
import logging
import os
from pathlib import Path

from aiohttp import WSMsgType, web

logger = logging.getLogger(__name__)


REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/5")

def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip().strip('"').strip("'"))
    except Exception:
        return default


TOKEN_TTL_SECONDS = _get_int_env("CAPTCHA_TOKEN_TTL_SECONDS", 600)
VNC_HOST_DEFAULT = os.getenv("CAPTCHA_VNC_HOST", "attendee-worker")


async def redis_get(token: str) -> dict | None:
    try:
        import redis.asyncio as redis

        r = redis.Redis.from_url(REDIS_URL)
        raw = await r.get(f"attendee:captcha:{token}")
        await r.aclose()
        if not raw:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        return json.loads(raw)
    except Exception as exc:
        logger.error("Failed to read captcha token from redis: %s", exc)
        return None


def get_novnc_dir() -> Path | None:
    # Debian/Ubuntu packages usually install noVNC to /usr/share/novnc.
    candidates = [
        Path("/usr/share/novnc"),
        Path("/usr/share/noVNC"),
        Path("/usr/share/novnc-core"),
    ]
    for p in candidates:
        if p.exists() and p.is_dir():
            return p
    return None


async def handle_root(request: web.Request) -> web.Response:
    return web.Response(
        text=(
            "Captcha console is not available without a token.\n\n"
            "Open the captcha link shown on the bot page (it looks like /captcha/<token>)."
        ),
        content_type="text/plain",
    )


async def handle_captcha(request: web.Request) -> web.Response:
    token = request.match_info.get("token")
    if not token:
        raise web.HTTPNotFound()

    info = await redis_get(token)
    if not info:
        return web.Response(text="Captcha link expired or invalid.", content_type="text/plain", status=404)

    # Use packaged noVNC HTML (vnc_lite.html is simplest) and point it to our websocket endpoint.
    # noVNC expects a websocket path that it appends to the current origin.
    ws_path = f"captcha/ws/{token}"
    html = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Captcha</title>
    <style>
      html, body {{ height: 100%; margin: 0; background: #111; }}
      .frame {{ position: fixed; inset: 0; }}
      iframe {{ width: 100%; height: 100%; border: 0; }}
    </style>
  </head>
  <body>
    <div class="frame">
      <iframe src="/novnc/vnc_lite.html?autoconnect=1&resize=remote&path={ws_path}"></iframe>
    </div>
  </body>
</html>
"""
    return web.Response(text=html, content_type="text/html")


async def websocket_proxy(request: web.Request) -> web.WebSocketResponse:
    token = request.match_info.get("token")
    if not token:
        raise web.HTTPNotFound()

    info = await redis_get(token)
    if not info:
        raise web.HTTPNotFound(text="Captcha token expired")

    vnc_host = info.get("vnc_host") or VNC_HOST_DEFAULT
    vnc_port = int(info.get("vnc_port") or 5900)

    ws = web.WebSocketResponse(autoping=True, heartbeat=20)
    await ws.prepare(request)

    try:
        reader, writer = await asyncio.open_connection(vnc_host, vnc_port)
    except Exception as exc:
        await ws.close(code=1011, message=f"Cannot connect to VNC server: {exc}".encode("utf-8"))
        return ws

    async def tcp_to_ws():
        try:
            while True:
                data = await reader.read(16_384)
                if not data:
                    break
                await ws.send_bytes(data)
        except Exception:
            pass
        finally:
            await ws.close()

    async def ws_to_tcp():
        try:
            async for msg in ws:
                if msg.type == WSMsgType.BINARY:
                    writer.write(msg.data)
                    await writer.drain()
                elif msg.type == WSMsgType.TEXT:
                    # noVNC should only send binary; ignore text.
                    continue
                elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                    break
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    await asyncio.gather(tcp_to_ws(), ws_to_tcp())
    return ws


def create_app() -> web.Application:
    app = web.Application()

    app.router.add_get("/captcha", handle_root)
    app.router.add_get("/captcha/{token}", handle_captcha)
    app.router.add_get("/captcha/ws/{token}", websocket_proxy)

    novnc_dir = get_novnc_dir()
    if novnc_dir:
        logger.info("Serving noVNC static from %s", novnc_dir)
        app.router.add_static("/novnc/", str(novnc_dir), show_index=False)
    else:
        logger.warning("noVNC directory not found; install 'novnc' package in the image")

    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    host = os.getenv("CAPTCHA_PROXY_HOST", "0.0.0.0")
    port = int(os.getenv("CAPTCHA_PROXY_PORT", "8080"))
    web.run_app(create_app(), host=host, port=port)


if __name__ == "__main__":
    main()
