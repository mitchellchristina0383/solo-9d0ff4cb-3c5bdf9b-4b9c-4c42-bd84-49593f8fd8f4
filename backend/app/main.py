"""模拟写入后端。

每个后端实例：
  * 把收到的写入落到自己的 SQLite（/data/writes.db），持久化保存；
  * 支持慢回执：POST /writes?delay=秒，先落库再延迟回执，模拟"已送达、未回执"；
  * 支持健康故障注入：POST /control/fault on/off，故障期间 /healthz 503、写入 500；
  * /writes/<id> 供代理在切流后仍向"原送达节点"查询回执；
  * /records 列出本节点全部写入记录；
  * POST /writes/<id>/purge 持久清除正文：就地把 payload 覆写为清除标记并落
    payload_purged/purged_at，写入 ID、节点归属、回执时间全部保留。幂等，
    重复清除只返回同一结果，不会恢复或重写任何正文。
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

import aiosqlite
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

NODE_NAME = os.environ.get("NODE_NAME", "backend")
DB_PATH = os.environ.get("BACKEND_DB", "/data/writes.db")

# 清除后的正文占位符：正文本体被就地覆写，任何读取路径都只能拿到该标记。
PURGE_MARKER = "***PURGED***"

# 故障开关是进程内瞬态，不需要落库。
_fault_on = False
# 正在慢回执中的请求 id -> asyncio.Event，用于 /writes/{id} 等待回执完成。
_pending: dict[str, asyncio.Event] = {}


async def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS writes (
                id           TEXT PRIMARY KEY,
                payload      TEXT NOT NULL,
                client_ip    TEXT,
                received_at  REAL NOT NULL,
                acked_at     REAL,
                delay        REAL NOT NULL DEFAULT 0
            )
            """
        )
        # 旧库迁移：补充清除状态列（新库直接补齐，ALTER 对已存在列会报错，先探测）。
        cur = await db.execute("PRAGMA table_info(writes)")
        cols = {r[1] for r in await cur.fetchall()}
        if "payload_purged" not in cols:
            await db.execute(
                "ALTER TABLE writes ADD COLUMN payload_purged INTEGER NOT NULL DEFAULT 0"
            )
        if "purged_at" not in cols:
            await db.execute("ALTER TABLE writes ADD COLUMN purged_at REAL")
        await db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(title=f"mock-write-backend-{NODE_NAME}", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> JSONResponse:
    if _fault_on:
        return JSONResponse(status_code=503, content={"node": NODE_NAME, "healthy": False})
    return JSONResponse(status_code=200, content={"node": NODE_NAME, "healthy": True})


@app.post("/control/fault")
async def control_fault(request: Request) -> dict[str, Any]:
    """注入/解除健康故障。body: {"on": true|false}"""
    global _fault_on
    body = await request.json()
    _fault_on = bool(body.get("on", True))
    return {"node": NODE_NAME, "fault": _fault_on}


@app.post("/writes")
async def create_write(request: Request) -> JSONResponse:
    if _fault_on:
        # 故障期间拒绝写入（代理探测会先失败，正常不会走到这里）。
        return JSONResponse(
            status_code=500,
            content={"node": NODE_NAME, "error": "node in fault state, write rejected"},
        )

    delay = float(request.query_params.get("delay", "0"))
    try:
        body = await request.json()
    except Exception:
        body = {}
    write_id = str(body.get("id") or uuid.uuid4())
    payload = str(body.get("payload", ""))
    now = time.time()

    # 先落库，再慢回执：即使回执期间连接被切，写入也已经在本节点持久化。
    # 若该 ID 已被清除，任何迟到/重复投递都不得把正文写回来：只补回执时间，
    # payload 保持清除标记。
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO writes (id, payload, client_ip, received_at, delay, "
            "payload_purged) VALUES (?, ?, ?, ?, ?, 0)",
            (write_id, payload, request.client.host if request.client else None, now, delay),
        )
        await db.execute(
            "UPDATE writes SET payload = CASE WHEN payload_purged = 1 THEN payload ELSE ? END, "
            "client_ip = COALESCE(client_ip, ?) WHERE id = ?",
            (payload, request.client.host if request.client else None, write_id),
        )
        await db.commit()

    if delay > 0:
        done = asyncio.Event()
        _pending[write_id] = done
        try:
            await asyncio.sleep(delay)
        finally:
            async with aiosqlite.connect(DB_PATH) as db:
                # 慢回执只更新回执时间，绝不触碰 payload——清除与慢回执交错时
                # 已清除的正文不会因此恢复。
                await db.execute(
                    "UPDATE writes SET acked_at = ? WHERE id = ?", (time.time(), write_id)
                )
                await db.commit()
            _pending.pop(write_id, None)
            done.set()
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE writes SET acked_at = ? WHERE id = ?", (time.time(), write_id)
            )
            await db.commit()

    return JSONResponse(
        status_code=201,
        content={
            "id": write_id,
            "node": NODE_NAME,
            "received_at": now,
            "delay": delay,
            "status": "acked",
        },
    )


@app.post("/writes/{write_id}/purge")
async def purge_write(write_id: str) -> JSONResponse:
    """持久清除单条写入的正文（保留 ID、节点归属与回执）。

    幂等：已清除的行再次调用只回显同一清除时间；行不存在返回 404，
    交由代理保留"待清除"并稍后重试（例如写入尚在投递途中）。
    """
    if _fault_on:
        # 故障期间明确拒绝，代理按未完成处理、保持待清除并继续重试。
        return JSONResponse(
            status_code=503,
            content={"node": NODE_NAME, "id": write_id,
                     "purged": False, "error": "node in fault state"},
        )

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, payload_purged, purged_at FROM writes WHERE id = ?", (write_id,)
        )
        row = await cur.fetchone()
        if row is None:
            return JSONResponse(
                status_code=404,
                content={"node": NODE_NAME, "id": write_id, "found": False, "purged": False},
            )
        if row["payload_purged"]:
            # 已清除：幂等回显，不改动任何字段。
            purged_at = row["purged_at"]
            already = True
        else:
            purged_at = time.time()
            await db.execute(
                "UPDATE writes SET payload = ?, payload_purged = 1, purged_at = ? "
                "WHERE id = ? AND payload_purged = 0",
                (PURGE_MARKER, purged_at, write_id),
            )
            await db.commit()
            already = False

    return JSONResponse(status_code=200, content={
        "node": NODE_NAME, "id": write_id, "found": True, "purged": True,
        "already_purged": already, "purged_at": purged_at,
        "payload": PURGE_MARKER,
    })


@app.get("/writes/{write_id}")
async def get_write(write_id: str, wait: bool = False) -> JSONResponse:
    """查询单条写入的回执状态。

    wait=true 时，如果该写入仍在慢回执中，最多等待 60 秒，供代理向原送达节点取回执。
    已清除的写入 payload 只返回清除标记，正文不会经此接口显露。
    """
    if wait and write_id in _pending:
        try:
            await asyncio.wait_for(_pending[write_id].wait(), timeout=60)
        except asyncio.TimeoutError:
            pass

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
        row = await cur.fetchone()
    if row is None:
        return JSONResponse(
            status_code=404, content={"node": NODE_NAME, "id": write_id, "found": False}
        )
    data = dict(row)
    data["node"] = NODE_NAME
    data["found"] = True
    data["status"] = "acked" if data["acked_at"] is not None else "pending"
    if data.get("payload_purged"):
        # 双保险：即便历史数据残留正文，读取层也只暴露标记。
        data["payload"] = PURGE_MARKER
    return JSONResponse(status_code=200, content=data)


@app.get("/records")
async def records() -> dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, payload, client_ip, received_at, acked_at, delay, "
            "payload_purged, purged_at FROM writes ORDER BY received_at"
        )
        rows = await cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        if d.get("payload_purged"):
            d["payload"] = PURGE_MARKER
        out.append(d)
    return {"node": NODE_NAME, "count": len(out), "records": out}
