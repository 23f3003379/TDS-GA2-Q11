import base64
import math
import time
import uuid
from collections import defaultdict, deque

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

TOTAL = 54          # T: catalog is IDs 1..54
RATE_LIMIT = 18     # R: requests per window per client
WINDOW = 10.0       # seconds

app = FastAPI()

CATALOG = [{"id": i, "name": f"Order {i}", "status": "confirmed"} for i in range(1, TOTAL + 1)]
idempotency_store: dict = {}                 # key -> order
client_hits = defaultdict(deque)             # client id -> timestamps of recent requests


# ---------- 3) per-client rate limiting ----------
@app.middleware("http")
async def rate_limit(request: Request, call_next):
    client_id = request.headers.get("x-client-id")
    if client_id and request.method != "OPTIONS":
        now = time.monotonic()
        q = client_hits[client_id]
        while q and now - q[0] >= WINDOW:    # drop timestamps older than the window
            q.popleft()
        if len(q) >= RATE_LIMIT:
            retry_after = max(1, math.ceil(WINDOW - (now - q[0])))
            return JSONResponse(
                status_code=429,
                content={"detail": "Too Many Requests"},
                headers={"Retry-After": str(retry_after)},
            )
        q.append(now)
    return await call_next(request)


# CORS is added AFTER the rate limiter, so it is the outer layer and
# the 429 responses also carry the CORS headers.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],                                   # Idempotency-Key, X-Client-Id
    expose_headers=["Retry-After", "Idempotent-Replayed"],  # so browser JS can read them
)


@app.get("/")
def health():
    return {"status": "ok"}


# ---------- 1) idempotent creation ----------
@app.post("/orders")
async def create_order(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}

    key = request.headers.get("idempotency-key")
    if not key:
        return JSONResponse(status_code=400, content={"detail": "Idempotency-Key header required"})

    # no await between this check and the store write, so it is atomic
    if key in idempotency_store:
        return JSONResponse(
            status_code=201,
            content=idempotency_store[key],
            headers={"Idempotent-Replayed": "true"},
        )

    order = {
        "id": "ord_" + uuid.uuid4().hex[:12],
        "idempotency_key": key,
        "status": "created",
        "data": body,
    }
    idempotency_store[key] = order
    return JSONResponse(status_code=201, content=order)


# ---------- 2) cursor pagination ----------
def encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(f"o:{offset}".encode()).decode()


def decode_cursor(cursor: str) -> int:
    raw = base64.urlsafe_b64decode(cursor.encode()).decode()
    if not raw.startswith("o:"):
        raise ValueError
    return max(0, int(raw[2:]))


@app.get("/orders")
def list_orders(limit: int = 10, cursor: str | None = None):
    limit = max(1, min(limit, 1000))
    offset = 0
    if cursor:
        try:
            offset = decode_cursor(cursor)
        except Exception:
            return JSONResponse(status_code=400, content={"detail": "invalid cursor"})

    items = CATALOG[offset: offset + limit]
    next_offset = offset + len(items)
    next_cursor = encode_cursor(next_offset) if next_offset < TOTAL else None
    return {"items": items, "next_cursor": next_cursor}
