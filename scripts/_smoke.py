#!/usr/bin/env python3
"""本地冒烟：在单进程内用 uvicorn 子进程拉起两后端+代理，跑核心清除流程后清理。

仅用于开发环境快速验证；完整断言见 scripts/selftest.py。
依赖通过 PYTHONUSERBASE（.pyuser）提供，子进程用 -E 保持隔离且能加载用户 site。
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
USERBASE = str(ROOT / ".pyuser") if (ROOT / ".pyuser" / "lib").exists() else None
PORTS = {"A": 18011, "B": 18012, "proxy": 18010}
PROXY = f"http://127.0.0.1:{PORTS['proxy']}"


def http(method, url, body=None, timeout=15):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def wait_ready(url, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.3)
    return False


def main():
    work = Path(tempfile.mkdtemp(prefix="purge-smoke-"))
    logs = work / "logs"
    logs.mkdir()
    procs = []
    base_env = {**os.environ, **({"PYTHONUSERBASE": USERBASE} if USERBASE else {})}

    def spawn(kind, app_dir, port, env):
        log = open(logs / f"{kind}.log", "wb")
        p = subprocess.Popen(
            [sys.executable, "-E", "-m", "uvicorn", "app.main:app",
             "--app-dir", str(app_dir), "--host", "127.0.0.1", "--port", str(port)],
            env={**base_env, **env}, stdout=log, stderr=subprocess.STDOUT,
        )
        procs.append(p)

    try:
        spawn("a", ROOT / "backend", PORTS["A"],
              {"NODE_NAME": "A", "BACKEND_DB": str(work / "a.db")})
        spawn("b", ROOT / "backend", PORTS["B"],
              {"NODE_NAME": "B", "BACKEND_DB": str(work / "b.db")})
        assert wait_ready(f"http://127.0.0.1:{PORTS['A']}/healthz"), "A not ready"
        assert wait_ready(f"http://127.0.0.1:{PORTS['B']}/healthz"), "B not ready"
        spawn("proxy", ROOT / "proxy", PORTS["proxy"], {
            "NODE_A_NAME": "A", "NODE_B_NAME": "B",
            "BACKEND_A_URL": f"http://127.0.0.1:{PORTS['A']}",
            "BACKEND_B_URL": f"http://127.0.0.1:{PORTS['B']}",
            "PROXY_DB": str(work / "proxy.db"),
            "WRITER_READ_TIMEOUT": "2", "CONNECT_TIMEOUT": "2",
            "PROBE_TIMEOUT": "2", "PURGE_POLL_INTERVAL": "1",
        })
        assert wait_ready(PROXY + "/"), "proxy not ready"

        # 1) 普通写入 + 清除完成
        code, w = http("POST", f"{PROXY}/api/writes?delay=0", {"payload": "secret-1"})
        assert code == 201 and w["status"] == "ACKED", w
        wid = w["id"]
        code, p = http("POST", f"{PROXY}/api/writes/{wid}/purge")
        print("purge healthy node:", code, p["purge_state"])
        assert code == 200 and p["purge_state"] == "PURGE_DONE", p
        assert p["write"]["payload"] == "***PURGED***", p["write"]

        # 各视图遮蔽核对
        st = http("GET", f"{PROXY}/api/status")[1]
        row = next(x for x in st["writes"] if x["id"] == wid)
        assert row["payload"] == "***PURGED***" and row["payload_purged"] == 1, row
        reca = http("GET", f"{PROXY}/api/nodes/A/records")[1]
        rr = next(x for x in reca["records"] if x["id"] == wid)
        assert rr["payload"] == "***PURGED***" and rr["payload_purged"] == 1, rr
        ref = http("GET", f"{PROXY}/api/writes/{wid}/refresh")[1]
        assert ref["backend_view"]["payload"] == "***PURGED***", ref
        print("OK: 状态页/节点记录/核实接口均遮蔽，ID/节点/回执保留:",
              row["id"][:8], row["target_node"], row["status"])

        # 2) 故障节点：保持待清除，后台接续
        w2 = http("POST", f"{PROXY}/api/writes?delay=0", {"payload": "secret-2"})[1]
        http("POST", f"{PROXY}/api/nodes/A/fault", {"on": True})
        code, p2 = http("POST", f"{PROXY}/api/writes/{w2['id']}/purge")
        print("purge faulty node:", code, p2["purge_state"], "attempts", p2["attempts"])
        assert code == 202 and p2["purge_state"] == "PURGE_PENDING", p2
        time.sleep(1.5)
        st = http("GET", f"{PROXY}/api/status")[1]
        assert st["purges"]["pending_count"] >= 1, st["purges"]
        # 故障期间代理侧仍遮蔽
        reca = http("GET", f"{PROXY}/api/nodes/A/records")[1]
        rr2 = next(x for x in reca["records"] if x["id"] == w2["id"])
        assert rr2["payload"] == "***PURGED***", rr2
        # 解除故障 -> 后台自动完成
        http("POST", f"{PROXY}/api/nodes/A/fault", {"on": False})
        end = time.time() + 10
        done = False
        while time.time() < end:
            st = http("GET", f"{PROXY}/api/status")[1]
            if all(x["write_id"] != w2["id"] for x in st["purges"]["pending"]):
                done = True
                break
            time.sleep(0.4)
        assert done, "reconciler did not complete purge after fault cleared"
        print("OK: 故障期待清除，解除后后台对账自动完成")

        print("\nSMOKE PASS")
    finally:
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
