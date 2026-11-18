"""代理侧 SQLite 持久层：节点配置、当前路由、写入记录、切流事件、正文清除单。

写入记录一旦落库即代表"已送达某节点"，回执状态只能就地向原节点查询更新，
任何状态机代码都不会把同一条写入再投到另一个节点。

正文清除（purge）语义：
  * 受理即在代理库把 payload 就地覆写为清除标记（遮蔽立即生效且持久，进程重启
    后依旧遮蔽），并开一张清除单 PURGE_PENDING，保留写入 ID、节点归属与回执；
  * 只有原送达节点也持久清除成功后，清除单才转为 PURGE_DONE；节点失联/中断时
    一直是 PURGE_PENDING，由后台对账循环与重复操作接续，直到完成。
"""
from __future__ import annotations

import os
import time
from typing import Any

import aiosqlite

DB_PATH = os.environ.get("PROXY_DB", "/data/proxy.db")

# 写入状态机
PENDING = "PENDING"        # 已送达/投递中，尚未拿到回执
ACKED = "ACKED"            # 已拿到原节点成功回执
TIMEOUT_UNCERTAIN = "TIMEOUT_UNCERTAIN"  # 写入已落库但回执超时，结果不确定
FAILED = "FAILED"          # 节点明确拒绝（连接前失败/5xx），未送达

# 清除状态机
PURGE_PENDING = "PURGE_PENDING"  # 代理已遮蔽，等待/重试原节点持久清除
PURGE_DONE = "PURGE_DONE"        # 原节点也已持久清除，全链路完成

# 清除后的正文占位符，与后端保持一致；正文本体不保留任何副本。
PURGE_MARKER = "***PURGED***"

# 单连接 + 单写锁，SQLite 写入串行化，避免 lock 竞争。
_conn: aiosqlite.Connection | None = None
_write_lock: Any = None


async def init_db() -> None:
    global _conn, _write_lock
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    _conn = aiosqlite.connect(DB_PATH)
    await _conn
    _write_lock = asyncio_lock()
    _conn.row_factory = aiosqlite.Row
    await _conn.execute("PRAGMA journal_mode=WAL")
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nodes (
            name        TEXT PRIMARY KEY,
            base_url    TEXT NOT NULL,
            created_at  REAL NOT NULL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS route (
            id          INTEGER PRIMARY KEY CHECK (id = 1),
            active      TEXT NOT NULL,
            updated_at  REAL NOT NULL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS writes (
            id           TEXT PRIMARY KEY,
            target_node  TEXT NOT NULL,
            payload      TEXT NOT NULL,
            delay_hint   REAL NOT NULL DEFAULT 0,
            status       TEXT NOT NULL,
            status_detail TEXT NOT NULL DEFAULT '',
            created_at   REAL NOT NULL,
            updated_at   REAL NOT NULL,
            acked_at     REAL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS purges (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            write_id      TEXT NOT NULL UNIQUE,
            target_node   TEXT NOT NULL,
            purge_state   TEXT NOT NULL,
            attempts      INTEGER NOT NULL DEFAULT 0,
            last_error   TEXT NOT NULL DEFAULT '',
            created_at    REAL NOT NULL,
            updated_at    REAL NOT NULL,
            purged_at     REAL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          REAL NOT NULL,
            kind        TEXT NOT NULL,
            message     TEXT NOT NULL
        )
        """
    )
    # 旧库迁移：writes 表补充清除标记列。
    cur = await _conn.execute("PRAGMA table_info(writes)")
    cols = {r[1] for r in await cur.fetchall()}
    if "payload_purged" not in cols:
        await _conn.execute(
            "ALTER TABLE writes ADD COLUMN payload_purged INTEGER NOT NULL DEFAULT 0"
        )
    if "purged_at" not in cols:
        await _conn.execute("ALTER TABLE writes ADD COLUMN purged_at REAL")
    await _conn.commit()


def asyncio_lock() -> Any:
    import asyncio

    return asyncio.Lock()


async def register_node(name: str, base_url: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT OR IGNORE INTO nodes (name, base_url, created_at) VALUES (?, ?, ?)",
            (name, base_url, time.time()),
        )
        # 已存在则更新地址（compose 重建容器场景）。
        await _conn.execute(
            "UPDATE nodes SET base_url = ? WHERE name = ?", (base_url, name)
        )
        cur = await _conn.execute("SELECT COUNT(*) AS c FROM route")
        row = await cur.fetchone()
        if row["c"] == 0:
            await _conn.execute(
                "INSERT INTO route (id, active, updated_at) VALUES (1, ?, ?)",
                (name, time.time()),
            )
        await _conn.commit()


async def get_active() -> str:
    cur = await _conn.execute("SELECT active FROM route WHERE id = 1")
    row = await cur.fetchone()
    return row["active"]


async def set_active(name: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "UPDATE route SET active = ?, updated_at = ? WHERE id = 1",
            (name, time.time()),
        )
        await _conn.commit()


async def list_nodes() -> list[dict[str, Any]]:
    cur = await _conn.execute("SELECT name, base_url FROM nodes ORDER BY name")
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def node_url(name: str) -> str | None:
    cur = await _conn.execute("SELECT base_url FROM nodes WHERE name = ?", (name,))
    row = await cur.fetchone()
    return row["base_url"] if row else None


async def insert_write(
    write_id: str, target_node: str, payload: str, delay_hint: float
) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT INTO writes (id, target_node, payload, delay_hint, status, "
            "status_detail, created_at, updated_at, payload_purged) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)",
            (
                write_id,
                target_node,
                payload,
                delay_hint,
                PENDING,
                "delivered, awaiting ack",
                time.time(),
                time.time(),
            ),
        )
        await _conn.commit()


async def update_write_status(
    write_id: str, status: str, detail: str, acked: bool = False
) -> None:
    async with _write_lock:
        acked_at = time.time() if acked else None
        if acked:
            await _conn.execute(
                "UPDATE writes SET status = ?, status_detail = ?, updated_at = ?, "
                "acked_at = ? WHERE id = ?",
                (status, detail, time.time(), acked_at, write_id),
            )
        else:
            await _conn.execute(
                "UPDATE writes SET status = ?, status_detail = ?, updated_at = ? "
                "WHERE id = ?",
                (status, detail, time.time(), write_id),
            )
        await _conn.commit()


async def get_write(write_id: str) -> dict[str, Any] | None:
    cur = await _conn.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
    row = await cur.fetchone()
    return _masked(dict(row)) if row else None


async def list_writes() -> list[dict[str, Any]]:
    cur = await _conn.execute("SELECT * FROM writes ORDER BY created_at DESC LIMIT 200")
    rows = await cur.fetchall()
    return [_masked(dict(r)) for r in rows]


def _masked(row: dict[str, Any]) -> dict[str, Any]:
    """读取层遮蔽：任何已清除（或历史标记残留）的行都不暴露正文。"""
    if row.get("payload_purged"):
        row["payload"] = PURGE_MARKER
    return row


# ---------------------------------------------------------------- 正文清除单

async def get_purge(write_id: str) -> dict[str, Any] | None:
    cur = await _conn.execute("SELECT * FROM purges WHERE write_id = ?", (write_id,))
    row = await cur.fetchone()
    return dict(row) if row else None


async def list_pending_purges() -> list[dict[str, Any]]:
    cur = await _conn.execute(
        "SELECT * FROM purges WHERE purge_state = ? ORDER BY id", (PURGE_PENDING,)
    )
    return [dict(r) for r in await cur.fetchall()]


async def list_done_purges(limit: int = 50) -> list[dict[str, Any]]:
    cur = await _conn.execute(
        "SELECT * FROM purges WHERE purge_state = ? ORDER BY id DESC LIMIT ?",
        (PURGE_DONE, limit),
    )
    return [dict(r) for r in await cur.fetchall()]


async def open_or_get_purge(write_id: str, target_node: str) -> tuple[dict[str, Any], bool]:
    """受理清除：先就地遮蔽代理侧正文，再开/取清除单。

    返回 (清除单行, 是否本次新建)。已存在的单子直接回显（重复操作幂等，
    不改变节点归属、不重置尝试计数）。
    """
    async with _write_lock:
        now = time.time()
        # 1) 代理侧正文立即、持久地遮蔽；写入 ID / target_node / 回执列原样保留。
        await _conn.execute(
            "UPDATE writes SET payload = ?, payload_purged = 1, "
            "purged_at = COALESCE(purged_at, ?), updated_at = ? WHERE id = ?",
            (PURGE_MARKER, now, now, write_id),
        )
        # 2) 开清除单（若已有则不动）。
        cur = await _conn.execute(
            "SELECT * FROM purges WHERE write_id = ?", (write_id,)
        )
        existing = await cur.fetchone()
        if existing is not None:
            await _conn.commit()
            return dict(existing), False
        await _conn.execute(
            "INSERT INTO purges (write_id, target_node, purge_state, attempts, "
            "last_error, created_at, updated_at) VALUES (?, ?, ?, 0, '', ?, ?)",
            (write_id, target_node, PURGE_PENDING, now, now),
        )
        await _conn.commit()
        cur = await _conn.execute("SELECT * FROM purges WHERE write_id = ?", (write_id,))
        return dict(await cur.fetchone()), True


async def mark_purge_attempt(write_id: str, error: str) -> None:
    """记录一次未成功的节点清除尝试（失联/拒绝/中断），状态保持待清除。"""
    async with _write_lock:
        await _conn.execute(
            "UPDATE purges SET attempts = attempts + 1, last_error = ?, "
            "updated_at = ? WHERE write_id = ? AND purge_state = ?",
            (error, time.time(), write_id, PURGE_PENDING),
        )
        await _conn.commit()


async def complete_purge(write_id: str) -> None:
    """原节点已持久清除：清除单转完成。幂等。"""
    async with _write_lock:
        now = time.time()
        await _conn.execute(
            "UPDATE purges SET purge_state = ?, last_error = '', updated_at = ?, "
            "purged_at = COALESCE(purged_at, ?) WHERE write_id = ?",
            (PURGE_DONE, now, now, write_id),
        )
        await _conn.execute(
            "UPDATE writes SET payload_purged = 1, payload = ?, purged_at = "
            "COALESCE(purged_at, ?), updated_at = ? WHERE id = ?",
            (PURGE_MARKER, now, now, write_id),
        )
        await _conn.commit()


async def add_event(kind: str, message: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT INTO events (ts, kind, message) VALUES (?, ?, ?)",
            (time.time(), kind, message),
        )
        await _conn.commit()


async def list_events(limit: int = 30) -> list[dict[str, Any]]:
    cur = await _conn.execute(
        "SELECT ts, kind, message FROM events ORDER BY id DESC LIMIT ?", (limit,)
    )
    rows = await cur.fetchall()
    return [dict(r) for r in rows]
