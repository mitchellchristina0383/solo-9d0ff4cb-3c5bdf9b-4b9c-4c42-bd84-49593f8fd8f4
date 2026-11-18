#!/usr/bin/env python3
"""验证旧版 SQLite（无清除列/清除表）能被新版平滑迁移，且历史数据完好。"""
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
work = Path(tempfile.mkdtemp(prefix="mig-"))

# 1) 构造旧版后端库
dba = work / "old-backend.db"
c = sqlite3.connect(dba)
c.execute(
    "CREATE TABLE writes (id TEXT PRIMARY KEY, payload TEXT NOT NULL, client_ip TEXT, "
    "received_at REAL NOT NULL, acked_at REAL, delay REAL NOT NULL DEFAULT 0)")
c.execute("INSERT INTO writes VALUES ('legacy-1','OLD-SECRET',NULL,1.0,1.0,0)")
c.commit()
c.close()

# 2) 构造旧版代理库
dbp = work / "old-proxy.db"
c = sqlite3.connect(dbp)
c.executescript(
    """
    CREATE TABLE nodes (name TEXT PRIMARY KEY, base_url TEXT NOT NULL, created_at REAL NOT NULL);
    INSERT INTO nodes VALUES ('A','http://127.0.0.1:19011',1.0);
    CREATE TABLE route (id INTEGER PRIMARY KEY CHECK(id=1), active TEXT NOT NULL, updated_at REAL NOT NULL);
    INSERT INTO route VALUES (1,'A',1.0);
    CREATE TABLE writes (id TEXT PRIMARY KEY, target_node TEXT NOT NULL, payload TEXT NOT NULL,
      delay_hint REAL NOT NULL DEFAULT 0, status TEXT NOT NULL, status_detail TEXT NOT NULL DEFAULT '',
      created_at REAL NOT NULL, updated_at REAL NOT NULL, acked_at REAL);
    INSERT INTO writes VALUES ('legacy-1','A','OLD-SECRET',0,'ACKED','x',1.0,1.0,1.0);
    CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL,
      kind TEXT NOT NULL, message TEXT NOT NULL);
    """)
c.commit()
c.close()

env = {**os.environ}
ub = ROOT / ".pyuser"
if (ub / "lib").exists():
    env["PYTHONUSERBASE"] = str(ub)

backend_code = (
    "import asyncio, os, sqlite3, sys\n"
    f"os.environ['BACKEND_DB']={str(dba)!r}\n"
    f"sys.path.insert(0, {str(ROOT / 'backend')!r})\n"
    "from app.main import init_db\n"
    "asyncio.run(init_db())\n"
    f"c=sqlite3.connect({str(dba)!r}); c.row_factory=sqlite3.Row\n"
    "cols=[r[1] for r in c.execute('PRAGMA table_info(writes)')]\n"
    "row=dict(c.execute('SELECT * FROM writes WHERE id=?',('legacy-1',)).fetchone())\n"
    "print('BACKEND_COLS', 'payload_purged' in cols and 'purged_at' in cols)\n"
    "print('BACKEND_ROW', row['payload']=='OLD-SECRET' and row['payload_purged']==0 "
    "and row['acked_at']==1.0)\n"
)
proxy_code = (
    "import asyncio, os, sqlite3, sys\n"
    f"os.environ['PROXY_DB']={str(dbp)!r}\n"
    f"sys.path.insert(0, {str(ROOT / 'proxy')!r})\n"
    "from app import db\n"
    "async def main():\n"
    "    await db.init_db()\n"
    "    await db._conn.close()\n"
    "asyncio.run(main())\n"
    f"c=sqlite3.connect({str(dbp)!r}); c.row_factory=sqlite3.Row\n"
    "cols=[r[1] for r in c.execute('PRAGMA table_info(writes)')]\n"
    "tables=[r[0] for r in c.execute(\"SELECT name FROM sqlite_master WHERE type='table'\")]\n"
    "row=dict(c.execute('SELECT * FROM writes WHERE id=?',('legacy-1',)).fetchone())\n"
    "print('PROXY_COLS', 'payload_purged' in cols and 'purged_at' in cols)\n"
    "print('PROXY_TABLE', 'purges' in tables)\n"
    "print('PROXY_ROW', row['payload']=='OLD-SECRET' and row['payload_purged']==0 "
    "and row['target_node']=='A')\n"
)

ok = True
for label, code in (("backend", backend_code), ("proxy", proxy_code)):
    r = subprocess.run([sys.executable, "-E", "-c", code], env=env,
                       capture_output=True, text=True, timeout=30)
    print(f"--- {label} (rc={r.returncode}) ---")
    print(r.stdout.strip())
    if r.stderr.strip():
        print("STDERR:", r.stderr.strip()[:400])
    ok = ok and r.returncode == 0 and "False" not in r.stdout

shutil.rmtree(work, ignore_errors=True)
print("MIGRATION", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
