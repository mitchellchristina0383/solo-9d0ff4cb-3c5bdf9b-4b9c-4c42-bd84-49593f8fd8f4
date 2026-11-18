"""FastAPI 反向代理 + 写入切流控制台 + 单笔正文清除。

切流安全语义（本文件的所有代码路径都遵守）：
  1. 每个写入请求在发出前快照"当前现役节点"，并把该节点名持久化到 writes 行；
     之后无论怎么切流，这个请求只与该节点通信，绝不重投到另一节点。
  2. 切流只改 route 表，对已经发往旧节点、尚未回执的在途请求不做任何取消动作；
     回执仍从旧节点读取（也可事后用 /api/writes/{id}/refresh 向原节点查询）。
  3. 切流前先探测目标节点 /healthz，探测失败则保留旧路由并返回 409。
  4. 读超时（写入大概率已在后端落库，只是回执没回来）标记为 TIMEOUT_UNCERTAIN，
     返回 504 并在页面提示"结果不确定"，代理不做任何自动补投/重试。

正文清除（purge）语义：
  5. 控制台按写入 ID 发起单笔清除；受理时代理库正文立即、持久遮蔽（保留写入 ID、
     节点归属与回执），状态页/节点记录页/核实接口立刻不再显露正文。
  6. 代理只向该写入"原送达节点"发持久清除；只有原节点也清除成功才显示完成。
     节点失联或清除中断一律保持"待清除"，后台对账循环 + 重复操作持续接续，
     代理重启后从 SQLite 恢复并继续。
  7. 清除与原写入投递、慢回执交错：正文在两侧都不会被重新写入或恢复显露；
     回执仍可向原节点核实；切流不改变清除单的节点归属。
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from . import db

NODE_A = os.environ.get("NODE_A_NAME", "A")
NODE_B = os.environ.get("NODE_B_NAME", "B")
URL_A = os.environ.get("BACKEND_A_URL", "http://127.0.0.1:8001")
URL_B = os.environ.get("BACKEND_B_URL", "http://127.0.0.1:8002")

# 连接失败 = 节点不可达、请求未送达；读超时 = 请求已发出、回执未归。
CONNECT_TIMEOUT = float(os.environ.get("CONNECT_TIMEOUT", "3.0"))
READ_TIMEOUT = float(os.environ.get("WRITER_READ_TIMEOUT", "30.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "2.5"))
# 后台清除对账循环间隔与单次清除请求超时。
PURGE_POLL_INTERVAL = float(os.environ.get("PURGE_POLL_INTERVAL", "2.5"))
PURGE_REQUEST_TIMEOUT = float(os.environ.get("PURGE_REQUEST_TIMEOUT", "5.0"))

# node -> 在途写入 id 集合（仅内存态，进程重启后以 DB 中 PENDING 行作为排空依据）。
_inflight: dict[str, set[str]] = {}
_inflight_lock = asyncio.Lock()

# 正在对原节点发起清除的写入 id（仅内存态，避免接口与对账循环重复打同一节点；
# 进程重启后由对账循环从 PURGE_PENDING 行恢复接续）。
_purge_inflight: set[str] = set()
# 清除对账协程的停止信号。
_purge_stop: asyncio.Event | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _purge_stop
    await db.init_db()
    await db.register_node(NODE_A, URL_A)
    await db.register_node(NODE_B, URL_B)
    _inflight.setdefault(NODE_A, set())
    _inflight.setdefault(NODE_B, set())
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT,
                              write=CONNECT_TIMEOUT, pool=CONNECT_TIMEOUT),
        # 显式关闭 httpx 默认的连接重试，保证"一次投递"语义。
        limits=httpx.Limits(max_connections=100),
    )
    pending = await db.list_pending_purges()
    await db.add_event("BOOT", f"proxy started, active={await db.get_active()}, "
                               f"purges resumed={len(pending)}")
    _purge_stop = asyncio.Event()
    purge_task = asyncio.create_task(_purge_reconciler())
    try:
        yield
    finally:
        _purge_stop.set()
        purge_task.cancel()
        try:
            await purge_task
        except (asyncio.CancelledError, Exception):
            pass
        await app.state.http.aclose()


app = FastAPI(title="write-cutover-proxy", lifespan=lifespan)


# ---------------------------------------------------------------- 健康探测

async def probe_node(base_url: str) -> tuple[bool, str]:
    """返回 (是否健康, 说明)。探测本身绝不触发任何写入。"""
    client: httpx.AsyncClient = app.state.http
    try:
        resp = await client.get(
            f"{base_url}/healthz", timeout=httpx.Timeout(PROBE_TIMEOUT)
        )
    except httpx.RequestError as exc:
        return False, f"probe request failed: {type(exc).__name__}: {exc}"
    if resp.status_code == 200:
        return True, "healthy"
    return False, f"healthz returned HTTP {resp.status_code}: {resp.text[:200]}"


async def node_health_map() -> dict[str, dict[str, str]]:
    nodes = await db.list_nodes()
    results = await asyncio.gather(*(probe_node(n["base_url"]) for n in nodes))
    return {
        n["name"]: {"healthy": ok, "detail": detail, "base_url": n["base_url"]}
        for n, (ok, detail) in zip(nodes, results)
    }


# ---------------------------------------------------------------- 切流

@app.post("/api/switch")
async def switch(request: Request) -> JSONResponse:
    body = await request.json()
    target = str(body.get("target", "")).strip()
    nodes = {n["name"]: n["base_url"] for n in await db.list_nodes()}
    if target not in nodes:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": f"unknown target node: {target!r}",
                     "known_nodes": sorted(nodes)},
        )

    active = await db.get_active()
    if target == active:
        return JSONResponse(content={"ok": True, "noop": True, "active": active,
                                     "message": "target is already active"})

    healthy, detail = await probe_node(nodes[target])
    if not healthy:
        # 探测失败：保持旧路由，不动 route 表。
        await db.add_event(
            "SWITCH_BLOCKED",
            f"{active} -> {target} blocked, target unhealthy: {detail}",
        )
        return JSONResponse(
            status_code=409,
            content={"ok": False, "active": active, "blocked": True,
                     "reason": f"target {target} probe failed: {detail}",
                     "message": "kept old route"},
        )

    async with _inflight_lock:
        inflight_snapshot = {k: len(v) for k, v in _inflight.items()}
        await db.set_active(target)
    await db.add_event(
        "SWITCH_OK",
        f"{active} -> {target} ok; inflight at cutover={inflight_snapshot}; "
        "new requests now route to target, in-flight requests stay on old node",
    )
    return JSONResponse(content={
        "ok": True, "active": target, "previous": active,
        "inflight_at_cutover": inflight_snapshot,
        "message": "switch done; in-flight writes continue against their original node",
    })


# ---------------------------------------------------------------- 写入代理

async def _track(node: str, write_id: str) -> None:
    async with _inflight_lock:
        _inflight.setdefault(node, set()).add(write_id)


async def _untrack(node: str, write_id: str) -> None:
    async with _inflight_lock:
        _inflight.setdefault(node, set()).discard(write_id)


@app.post("/api/writes")
async def proxy_write(request: Request) -> JSONResponse:
    params = request.query_params
    delay = float(params.get("delay", "0"))
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {"payload": body}

    # 关键：路由在投递前快照，切流不影响本请求。
    active = await db.get_active()
    base_url = await db.node_url(active)
    write_id = str(body.get("id") or uuid.uuid4())
    body["id"] = write_id
    payload = str(body.get("payload", ""))

    await db.insert_write(write_id, active, payload, delay)
    await _track(active, write_id)
    client: httpx.AsyncClient = app.state.http
    try:
        resp = await client.post(
            f"{base_url}/writes", params={"delay": delay}, json=body
        )
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout,
            httpx.WriteTimeout) as exc:
        # 连接层面失败：请求没有被后端受理，标记 FAILED，不向任何节点补投。
        detail = f"connect to {active} failed: {type(exc).__name__}: {exc}"
        await db.update_write_status(write_id, db.FAILED, detail)
        await db.add_event("WRITE_FAILED", f"{write_id} -> {active}: {detail}")
        return JSONResponse(status_code=502,
                            content={"id": write_id, "node": active,
                                     "status": db.FAILED, "detail": detail})
    except httpx.ReadTimeout:
        # 读超时：请求已送达，后端可能已落库，回执未归。结果不确定，禁止补投。
        detail = (f"read timeout after {READ_TIMEOUT}s waiting ack from {active}; "
                  "write may already be persisted there, result UNCERTAIN; "
                  "no automatic re-delivery to another node")
        await db.update_write_status(write_id, db.TIMEOUT_UNCERTAIN, detail)
        await db.add_event("WRITE_TIMEOUT_UNCERTAIN",
                           f"{write_id} -> {active}: {detail}")
        return JSONResponse(status_code=504,
                            content={"id": write_id, "node": active,
                                     "status": db.TIMEOUT_UNCERTAIN,
                                     "uncertain": True, "detail": detail})
    except httpx.HTTPError as exc:
        detail = f"unexpected transport error against {active}: {exc}"
        await db.update_write_status(write_id, db.FAILED, detail)
        return JSONResponse(status_code=502,
                            content={"id": write_id, "node": active,
                                     "status": db.FAILED, "detail": detail})
    finally:
        await _untrack(active, write_id)

    if resp.status_code in (200, 201):
        try:
            ack = resp.json()
        except Exception:
            ack = {}
        await db.update_write_status(
            write_id, db.ACKED,
            f"acked by {active} (backend status={ack.get('status', 'acked')})",
            acked=True,
        )
        await db.add_event("WRITE_ACKED", f"{write_id} acked by {active}")
        return JSONResponse(status_code=201,
                            content={"id": write_id, "node": active,
                                     "status": db.ACKED, "backend": ack})

    # 后端明确拒绝（如故障态 500）：未受理，FAILED；同样不补投。
    detail = f"{active} rejected with HTTP {resp.status_code}: {resp.text[:300]}"
    await db.update_write_status(write_id, db.FAILED, detail)
    await db.add_event("WRITE_FAILED", f"{write_id} -> {active}: {detail}")
    return JSONResponse(
        status_code=502,
        content={"id": write_id, "node": active, "status": db.FAILED, "detail": detail},
    )


@app.get("/api/writes/{write_id}/refresh")
async def refresh_write(write_id: str) -> JSONResponse:
    """向"原送达节点"查询该写入的最新回执状态——切流后仍然找旧节点。"""
    row = await db.get_write(write_id)
    if row is None:
        return JSONResponse(status_code=404,
                            content={"error": f"unknown write {write_id}"})
    node = row["target_node"]
    base_url = await db.node_url(node)
    client: httpx.AsyncClient = app.state.http
    try:
        resp = await client.get(
            f"{base_url}/writes/{write_id}",
            timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=10.0,
                                  write=CONNECT_TIMEOUT, pool=CONNECT_TIMEOUT),
        )
    except httpx.RequestError as exc:
        detail = f"refresh query to original node {node} failed: {type(exc).__name__}"
        await db.update_write_status(write_id, row["status"],
                                     f"{row['status_detail']} | {detail}")
        refreshed = await db.get_write(write_id)
        return JSONResponse(status_code=200,
                            content={"refreshed": False, "write": refreshed,
                                     "note": detail})

    if resp.status_code == 404:
        note = (f"original node {node} has no row yet; keep status {row['status']} "
                "(do not re-deliver)")
        return JSONResponse(content={"refreshed": False, "write": row, "note": note})

    data = resp.json()
    if data.get("status") == "acked" and row["status"] != db.ACKED:
        await db.update_write_status(
            write_id, db.ACKED,
            f"acked confirmed later by original node {node}", acked=True
        )
        await db.add_event("WRITE_RESOLVED",
                           f"{write_id} confirmed acked by original node {node}")
    # 核实接口同样不得显露已清除的正文：只回显清除标记与清除状态。
    purge = await db.get_purge(write_id)
    if purge is not None and isinstance(data, dict):
        data["payload"] = db.PURGE_MARKER
        data["payload_purged"] = True
        data["purge_state"] = purge["purge_state"]
    refreshed = await db.get_write(write_id)
    return JSONResponse(content={"refreshed": True, "write": refreshed,
                                 "backend_view": data})


# ---------------------------------------------------------------- 状态/排空

@app.get("/api/status")
async def status() -> JSONResponse:
    active = await db.get_active()
    health = await node_health_map()
    writes = await db.list_writes()

    async with _inflight_lock:
        inflight_counts = {k: len(v) for k, v in _inflight.items()}
        inflight_ids = {k: sorted(v) for k, v in _inflight.items()}

    # 排空视角：已切走的旧节点上仍未拿到回执的写入。
    per_node: dict[str, dict[str, Any]] = {}
    for w in writes:
        slot = per_node.setdefault(
            w["target_node"], {"total": 0, "pending": 0, "acked": 0,
                               "timeout_uncertain": 0, "failed": 0}
        )
        slot["total"] += 1
        slot[w["status"].lower()] = slot.get(w["status"].lower(), 0) + 1
    for name in health:
        per_node.setdefault(name, {"total": 0, "pending": 0, "acked": 0,
                                   "timeout_uncertain": 0, "failed": 0})
    for name, slot in per_node.items():
        slot["inflight_now"] = inflight_counts.get(name, 0)
        slot["inflight_ids"] = inflight_ids.get(name, [])
        slot["is_active"] = name == active
        slot["draining"] = name != active and (
            slot["inflight_now"] > 0 or slot["pending"] > 0
        )

    return JSONResponse(content={
        "ts": time.time(),
        "active": active,
        "nodes": health,
        "drainage": per_node,
        "writes": writes,
        "purges": await _purge_view(),
        "events": await db.list_events(),
        "timeouts": {"connect": CONNECT_TIMEOUT, "read": READ_TIMEOUT,
                     "probe": PROBE_TIMEOUT},
    })


async def _purge_view() -> dict[str, Any]:
    """清除视角：待清除/已完成清单，便于排空核对。"""
    pending = await db.list_pending_purges()
    done = await db.list_done_purges()
    return {
        "pending_count": len(pending),
        "done_count": len(done),
        "pending": pending,
        "recent_done": done,
    }


@app.get("/api/nodes/{name}/records")
async def node_records(name: str) -> JSONResponse:
    """透传某后端自己保存的写入记录。

    已受理清除的写入，在节点记录页也立即遮蔽正文——即便清除请求尚在途中、
    后端还没来得及持久清除，代理侧也不会把原文透出来。
    """
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    try:
        resp = await app.state.http.get(
            f"{base_url}/records", timeout=httpx.Timeout(PROBE_TIMEOUT)
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    data = resp.json()
    masked_ids = {
        p["write_id"] for p in (await db.list_pending_purges())
    } | {
        w["id"] for w in await db.list_writes() if w.get("payload_purged")
    }
    for rec in data.get("records", []):
        if rec.get("id") in masked_ids or rec.get("payload_purged"):
            rec["payload"] = db.PURGE_MARKER
            rec["payload_purged"] = True
    return JSONResponse(content=data)


@app.post("/api/nodes/{name}/fault")
async def node_fault(name: str, request: Request) -> JSONResponse:
    """给某后端注入/解除健康故障（透传后端控制接口）。"""
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    body = await request.json()
    try:
        resp = await app.state.http.post(
            f"{base_url}/control/fault", json={"on": bool(body.get("on", True))},
            timeout=httpx.Timeout(PROBE_TIMEOUT),
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    await db.add_event("FAULT_INJECTION",
                       f"node {name} fault on={bool(body.get('on', True))}")
    return JSONResponse(content=resp.json())


# ---------------------------------------------------------------- 正文清除

async def _attempt_node_purge(
    write_id: str, node: str
) -> tuple[bool, str, bool]:
    """向原送达节点发起一次持久清除。

    返回 (是否完成, 说明, 节点是否无此行)。
      * 节点 200 确认已清除 -> 完成；
      * 节点 404 -> not_found=True（该行当前不在节点上；是否算完成由调用方结合
        写入是否仍在途判断，避免在途行稍后带着原文落库）；
      * 传输错误/超时/其它响应 -> 未完成，保持待清除，后续重试。
    """
    base_url = await db.node_url(node)
    if base_url is None:
        return False, f"unknown node {node}", False
    try:
        resp = await app.state.http.post(
            f"{base_url}/writes/{write_id}/purge",
            timeout=httpx.Timeout(connect=CONNECT_TIMEOUT,
                                  read=PURGE_REQUEST_TIMEOUT,
                                  write=CONNECT_TIMEOUT, pool=CONNECT_TIMEOUT),
        )
    except httpx.RequestError as exc:
        return False, f"purge request to {node} failed: {type(exc).__name__}: {exc}", False

    if resp.status_code == 200:
        try:
            body = resp.json()
        except Exception:
            body = {}
        if body.get("purged"):
            return True, f"node {node} payload purged", False
        return False, f"node {node} returned 200 without purge confirmation", False
    if resp.status_code == 404:
        return False, f"node {node} has no row yet", True
    return (False,
            f"node {node} purge rejected HTTP {resp.status_code}: {resp.text[:200]}",
            False)


async def run_purge(write_id: str) -> dict[str, Any]:
    """执行一次清除尝试并落库状态；供清除接口与对账循环共用。"""
    purge = await db.get_purge(write_id)
    if purge is None:
        return {"write_id": write_id, "purge_state": db.PURGE_PENDING,
                "ok": False, "error": "purge ticket missing"}
    if purge["purge_state"] == db.PURGE_DONE:
        return {"write_id": write_id, "purge_state": db.PURGE_DONE,
                "ok": True, "already_done": True}

    if write_id in _purge_inflight:
        return {"write_id": write_id, "purge_state": db.PURGE_PENDING,
                "ok": False, "in_flight": True}
    _purge_inflight.add(write_id)
    try:
        ok, detail, not_found = await _attempt_node_purge(
            write_id, purge["target_node"])
        if ok:
            await db.complete_purge(write_id)
            await db.add_event("PURGE_DONE", f"{write_id}: {detail}")
            return {"write_id": write_id, "purge_state": db.PURGE_DONE,
                    "ok": True, "detail": detail}

        if not_found:
            write_row = await db.get_write(write_id)
            # 只有在写入已定稿、且不可能再在节点落行时，404 才可靠。
            #   PENDING：投递在途，节点行可能稍后带着原文落库；
            #   TIMEOUT_UNCERTAIN：请求已送达，行可能已在节点、只是尚未可查；
            # 这两种情况下保持待清除并继续重试，绝不把 404 当作完成。
            unsettled = write_row is not None and write_row["status"] in (
                db.PENDING, db.TIMEOUT_UNCERTAIN)
            if unsettled:
                note = ("node row not found while write not settled "
                        f"({write_row['status']}); will retry")
                await db.mark_purge_attempt(write_id, note)
                fresh = await db.get_purge(write_id)
                return {"write_id": write_id, "purge_state": db.PURGE_PENDING,
                        "ok": False, "error": note, "attempts": fresh["attempts"]}
            # ACKED/FAILED 定稿后节点确认无行：节点侧本就无正文（如连接前 FAILED）。
            await db.complete_purge(write_id)
            await db.add_event(
                "PURGE_DONE",
                f"{write_id}: node has no row after write settled (nothing persisted)")
            return {"write_id": write_id, "purge_state": db.PURGE_DONE,
                    "ok": True, "detail": "node has no row; nothing persisted there"}

        await db.mark_purge_attempt(write_id, detail)
        await db.add_event("PURGE_RETRY", f"{write_id} still pending: {detail}")
        fresh = await db.get_purge(write_id)
        return {"write_id": write_id, "purge_state": db.PURGE_PENDING,
                "ok": False, "error": detail, "attempts": fresh["attempts"]}
    finally:
        _purge_inflight.discard(write_id)


async def _purge_reconciler() -> None:
    """后台对账：持续接续所有待清除单。

    节点失联、清除中断或代理重启后，只要 SQLite 里还是 PURGE_PENDING，
    这里就按固定间隔向原送达节点重试，直到节点持久清除成功（或确认本就无行）。
    """
    assert _purge_stop is not None
    while not _purge_stop.is_set():
        try:
            for purge in await db.list_pending_purges():
                if _purge_stop.is_set():
                    break
                await run_purge(purge["write_id"])
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # 对账循环本身绝不因单条异常而退出
            try:
                await db.add_event("PURGE_RECONCILER_ERROR", repr(exc)[:300])
            except Exception:
                pass
        try:
            await asyncio.wait_for(_purge_stop.wait(), timeout=PURGE_POLL_INTERVAL)
        except asyncio.TimeoutError:
            continue


@app.post("/api/writes/{write_id}/purge")
async def purge_write(write_id: str) -> JSONResponse:
    """控制台按 ID 发起单笔正文清除。

    受理即遮蔽代理侧正文并开清除单（幂等：重复发起回显同一单子）；随后立即
    向原送达节点尝试一次持久清除，失败不报错给用户，保持"待清除"由后台接续。
    """
    row = await db.get_write(write_id)
    if row is None:
        return JSONResponse(status_code=404,
                            content={"ok": False, "error": f"unknown write {write_id}"})
    node = row["target_node"]  # 清除单始终绑定原送达节点，切流不改变归属。
    purge, created = await db.open_or_get_purge(write_id, node)
    if created:
        await db.add_event(
            "PURGE_REQUESTED",
            f"{write_id} accepted: proxy payload masked immediately, node={node}",
        )

    result = await run_purge(write_id)
    # 重新读取，确保返回的 write/purge 与库一致（正文已是标记）。
    write_view = await db.get_write(write_id)
    purge_view = await db.get_purge(write_id)
    status_code = 200 if result.get("ok") else 202
    return JSONResponse(status_code=status_code, content={
        "ok": True,
        "write_id": write_id,
        "node": node,
        "purge_state": purge_view["purge_state"],
        "masked": True,
        "created": created,
        "attempts": purge_view["attempts"],
        "last_error": purge_view["last_error"],
        "write": write_view,
        "message": ("original node payload purged; purge complete"
                    if result.get("ok")
                    else "proxy payload masked; waiting for original node purge "
                         "(will keep retrying)"),
    })


# ---------------------------------------------------------------- 控制台页面

@app.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    html = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")
    return HTMLResponse(html)
