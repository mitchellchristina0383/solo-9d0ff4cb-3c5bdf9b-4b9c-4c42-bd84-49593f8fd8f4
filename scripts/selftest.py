#!/usr/bin/env python3
"""端到端自测（仅依赖标准库）：用临时目录拉起两个后端 + 一个代理并逐项核验。

覆盖两部分：
  切流（原有）：
  1. 普通写入落现役节点，备用节点无该写入（无重复投递）；
  2. 慢回执写入在途时切流：切流立即生效，新请求走新节点，
     在途写入仍由旧节点回执，且只存在于旧节点；
  3. 目标节点健康故障时切流被拦截（409），旧路由保持；
  4. 连续两次切流都不打断各自的在途写入（再次切流安全）；
  5. 回执超时：返回 504 + TIMEOUT_UNCERTAIN，不自动补投；
     事后向"原送达节点"查询可确认其实已落库并回执；
  6. 全量核对：两侧后端写入 ID 集合不相交，每条写入恰好出现一次。

  正文清除（新增）：
  7. 按 ID 清除：受理即遮蔽，原节点持久清除后 PURGE_DONE；ID/节点/回执保留；
  8. 状态页、节点记录页、核实接口三处立即遮蔽，无原文泄漏；
  9. 节点故障：保持 PURGE_PENDING，解除后后台对账自动完成；
 10. 代理重启：待清除单从 SQLite 恢复并继续，重启后仍遮蔽；
 11. 清除与在途写入交错（先清后到）：正文不会重新写入，最终完成；
 12. 清除与慢回执交错：正文不恢复，回执仍可向原节点核实；
 13. 切流不改变清除单归属；重复清除幂等；
 14. 直连后端核对：两侧 SQLite 中被清行 payload 均为清除标记、无原文残留。

用法:
  python3 scripts/selftest.py [--python /path/to/python]

说明：子进程使用 -E（忽略 PYTHON* 环境变量但仍加载用户 site-packages），
这样既保持环境隔离（不被当前目录/环境变量里的同名模块污染），又能加载
通过 PYTHONUSERBASE 安装的 fastapi/httpx 等依赖；Docker 内依赖在系统
site-packages，-E 与 -I 行为一致。
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PASS, FAIL = 0, 0
RESULTS: list[tuple[bool, str]] = []
PURGE_MARKER = "***PURGED***"


def _detect_pythonuserbase() -> str | None:
    """探测本地依赖目录（.pyuser）。

    本地开发机若依赖装在仓库内的 .pyuser 用户 site，则把它经 PYTHONUSERBASE
    传给 -E 子进程；Docker 内依赖位于系统 site-packages 时返回 None，无需设置。
    """
    candidate = ROOT / ".pyuser"
    if (candidate / "lib").exists():
        return str(candidate)
    return None


# 子进程环境在 spawn 时统一合并该变量。
_EXTRA_ENV = {"PYTHONUSERBASE": ub} if (ub := _detect_pythonuserbase()) else {}


def check(cond: bool, name: str, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        RESULTS.append((True, f"PASS  {name}" + (f"  ({detail})" if detail else "")))
        print(f"  \033[32mPASS\033[0m  {name} {detail}")
    else:
        FAIL += 1
        RESULTS.append((False, f"FAIL  {name}  {detail}"))
        print(f"  \033[31mFAIL\033[0m  {name} {detail}")


def http(method: str, url: str, body: dict | None = None, timeout: float = 30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def wait_ready(url: str, timeout: float = 20) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.3)
    return False


def wait_status(proxy: str, write_id: str, expect: str, timeout: float = 15) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, s = http("GET", f"{proxy}/api/status")
        for w in s["writes"]:
            if w["id"] == write_id and w["status"] == expect:
                return w
        time.sleep(0.15)
    raise AssertionError(f"write {write_id} never reached status {expect}")


def wait_purge_state(proxy: str, write_id: str, expect: str,
                     timeout: float = 15) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, s = http("GET", f"{proxy}/api/status")
        for p in s["purges"]["pending"] + s["purges"]["recent_done"]:
            if p["write_id"] == write_id and p["purge_state"] == expect:
                return p
        time.sleep(0.2)
    raise AssertionError(f"purge of {write_id} never reached {expect}")


def find_write(proxy: str, write_id: str) -> dict:
    _, s = http("GET", f"{proxy}/api/status")
    return next(w for w in s["writes"] if w["id"] == write_id)


def find_record(proxy: str, node: str, write_id: str) -> dict:
    _, rec = http("GET", f"{proxy}/api/nodes/{node}/records")
    return next(r for r in rec["records"] if r["id"] == write_id)


class SlowPost(threading.Thread):
    def __init__(self, proxy: str, delay: float, payload: str):
        super().__init__(daemon=True)
        self.proxy, self.delay, self.payload = proxy, delay, payload
        self.result: tuple[int, dict] | None = None
        self.write_id: str | None = None

    def run(self):
        self.result = http("POST", f"{self.proxy}/api/writes?delay={self.delay}",
                           {"payload": self.payload}, timeout=30)
        if self.result:
            self.write_id = self.result[1].get("id")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--python", default=sys.executable)
    args = ap.parse_args()

    work = Path(tempfile.mkdtemp(prefix="cutover-selftest-"))
    logs = work / "logs"
    logs.mkdir()
    backends: list[subprocess.Popen] = []
    proxy_proc: subprocess.Popen | None = None
    ports = {"proxy": 18100, "A": 18101, "B": 18102}
    proxy = f"http://127.0.0.1:{ports['proxy']}"
    proxy_db = work / "proxy.db"

    proxy_env = {
        "NODE_A_NAME": "A", "NODE_B_NAME": "B",
        "BACKEND_A_URL": f"http://127.0.0.1:{ports['A']}",
        "BACKEND_B_URL": f"http://127.0.0.1:{ports['B']}",
        "PROXY_DB": str(proxy_db),
        "WRITER_READ_TIMEOUT": "2", "CONNECT_TIMEOUT": "2", "PROBE_TIMEOUT": "2",
        "PURGE_POLL_INTERVAL": "0.7",
    }

    def spawn_backend(kind: str, app_dir: Path, env: dict, port: int) -> None:
        log = open(logs / f"{kind}.log", "wb")
        p = subprocess.Popen(
            [args.python, "-E", "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port), "--app-dir", str(app_dir)],
            env={**os.environ, **_EXTRA_ENV, **env}, stdout=log, stderr=subprocess.STDOUT,
        )
        backends.append(p)

    def spawn_proxy() -> subprocess.Popen:
        nonlocal proxy_proc
        log = open(logs / "proxy.log", "ab")
        proxy_proc = subprocess.Popen(
            [args.python, "-E", "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(ports["proxy"]),
             "--app-dir", str(ROOT / "proxy")],
            env={**os.environ, **_EXTRA_ENV, **proxy_env}, stdout=log, stderr=subprocess.STDOUT,
        )
        return proxy_proc

    def stop_proxy() -> None:
        nonlocal proxy_proc
        if proxy_proc is None:
            return
        proxy_proc.send_signal(signal.SIGTERM)
        try:
            proxy_proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proxy_proc.kill()
        proxy_proc = None

    try:
        print("== 启动两个后端 + 代理（读回执超时 2s，便于演练超时场景） ==")
        spawn_backend("backend-a", ROOT / "backend",
                      {"NODE_NAME": "A", "BACKEND_DB": str(work / "a.db")}, ports["A"])
        spawn_backend("backend-b", ROOT / "backend",
                      {"NODE_NAME": "B", "BACKEND_DB": str(work / "b.db")}, ports["B"])
        ok_a = wait_ready(f"http://127.0.0.1:{ports['A']}/healthz")
        ok_b = wait_ready(f"http://127.0.0.1:{ports['B']}/healthz")
        check(ok_a and ok_b, "两个后端启动并健康", f"A={ok_a} B={ok_b}")

        spawn_proxy()
        check(wait_ready(proxy + "/"), "代理启动并提供控制台页面")

        _, st = http("GET", f"{proxy}/api/status")
        check(st["active"] == "A", "初始现役节点为 A", f"active={st['active']}")

        print("\n== 场景 1：普通写入只落现役节点 A ==")
        code, w0 = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "base-write"})
        check(code == 201 and w0["status"] == "ACKED", "普通写入成功回执",
              f"id={w0.get('id','')[:8]} node={w0.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        check(any(r["id"] == w0["id"] for r in rec_a["records"]), "写入存在于 A 的 SQLite")
        check(not any(r["id"] == w0["id"] for r in rec_b["records"]), "B 上没有该写入（未重复投递）")

        print("\n== 场景 2：慢回执在途切流 A -> B，不中断、不重复 ==")
        slow1 = SlowPost(proxy, 1.2, "slow-during-cutover-1")
        slow1.start()
        deadline = time.time() + 5
        pending_id = None
        while time.time() < deadline:
            _, s = http("GET", f"{proxy}/api/status")
            for row in s["writes"]:
                if row["status"] == "PENDING":
                    pending_id = row["id"]
                    inflight = s["drainage"]["A"]["inflight_now"]
                    check(inflight >= 1, "切流前 A 显示在途写入", f"inflight_now={inflight}")
                    break
            if pending_id:
                break
            time.sleep(0.1)
        check(bool(pending_id), "慢写入已进入 PENDING（已送达、待回执）")

        code, sw = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200 and sw["ok"] and sw["active"] == "B", "在途切流 A -> B 成功")
        _, s2 = http("GET", f"{proxy}/api/status")
        check(s2["drainage"]["A"]["draining"] is True, "旧节点 A 进入排空调（DRAINING）")

        slow1.join(timeout=10)
        check(not slow1.is_alive() and slow1.result and slow1.result[0] == 201,
              "在途写入未被切流打断，仍正常拿到回执")
        check(slow1.write_id == pending_id and slow1.result[1]["node"] == "A",
              "回执仍来自原送达节点 A",
              f"node={slow1.result[1].get('node') if slow1.result else '?'}")
        row = wait_status(proxy, pending_id, "ACKED")
        check(row["target_node"] == "A", "代理记录中该写入始终绑定 A")

        code, w_new = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "after-switch"})
        check(code == 201 and w_new["node"] == "B", "切流后新请求立即走新节点 B",
              f"node={w_new.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(pending_id in ids_a and pending_id not in ids_b, "慢写入只在 A，未被投到 B")
        check(w_new["id"] in ids_b and w_new["id"] not in ids_a, "新写入只在 B")

        print("\n== 场景 3：目标节点探测失败 -> 保持旧路由 ==")
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": True})
        code, blocked = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 409 and blocked.get("blocked") and blocked.get("active") == "B",
              "A 故障时切向 A 被 409 拦截，现役仍为 B", blocked.get("reason", "")[:90])
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": False})
        code, sw2 = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200 and sw2["active"] == "A", "故障解除后切流成功，现役回到 A")

        print("\n== 场景 4：连续再次切流，各自在途写入都绑定原节点 ==")
        t1 = SlowPost(proxy, 1.2, "roundtrip-a")
        t1.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200, "第二次切流 A -> B 成功且不打断在途写入")
        t1.join(timeout=10)
        check(t1.result and t1.result[1]["node"] == "A", "切走前送达 A 的写入仍由 A 回执")

        t2 = SlowPost(proxy, 1.2, "roundtrip-b")
        t2.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200, "第三次切流 B -> A 成功")
        t2.join(timeout=10)
        check(t2.result and t2.result[1]["node"] == "B",
              "切走前送达 B 的写入仍由 B 回执（再次切流不打断它）")

        print("\n== 场景 5：回执超时 -> 结果不确定，不补投，可事后向原节点核实 ==")
        active_before = http("GET", f"{proxy}/api/status")[1]["active"]
        t0 = time.time()
        code, tout = http("POST", f"{proxy}/api/writes?delay=5",
                          {"payload": "timeout-case"}, timeout=15)
        elapsed = time.time() - t0
        check(code == 504 and tout.get("uncertain") and
              tout["status"] == "TIMEOUT_UNCERTAIN",
              f"读超时返回 504 + TIMEOUT_UNCERTAIN（{elapsed:.1f}s 触发）")
        timeout_id, timeout_node = tout["id"], tout["node"]
        row = find_write(proxy, timeout_id)
        check(row["status"] == "TIMEOUT_UNCERTAIN", "超时状态已落库，页面可见结果不确定")
        _, rec_other = http("GET", f"{proxy}/api/nodes/"
                            f"{'B' if timeout_node == 'A' else 'A'}/records")
        check(not any(r["id"] == timeout_id for r in rec_other["records"]),
              "超时后代理没有向另一节点补投")

        print("   等待后端慢回执结束后，向原送达节点查询核实……")
        time.sleep(4.2)
        code, ref = http("GET", f"{proxy}/api/writes/{timeout_id}/refresh")
        check(code == 200 and ref["write"]["status"] == "ACKED"
              and ref["write"]["target_node"] == timeout_node,
              "向原节点查询后确认：写入其实已落库并回执，状态就地更正，未换节点")

        # ============================================================
        #                     正文清除（purge）
        # ============================================================
        print("\n== 场景 7：按 ID 清除，受理即遮蔽、原节点清除后完成，保留 ID/节点/回执 ==")
        _, wp = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "TOP-SECRET-7"})
        pid7 = wp["id"]
        node7 = wp["node"]
        code, p7 = http("POST", f"{proxy}/api/writes/{pid7}/purge")
        check(code == 200 and p7["purge_state"] == "PURGE_DONE",
              "健康节点清除同步完成 PURGE_DONE", f"http={code}")
        check(p7["write"]["payload"] == PURGE_MARKER, "清除响应中的正文已遮蔽")
        row7 = find_write(proxy, pid7)
        check(row7["id"] == pid7 and row7["target_node"] == node7
              and row7["status"] == "ACKED", "写入 ID、节点归属、回执均保留")
        check(row7["payload"] == PURGE_MARKER and row7["payload_purged"] == 1,
              "代理库正文就地遮蔽")
        rec7 = find_record(proxy, node7, pid7)
        check(rec7["payload"] == PURGE_MARKER and rec7["payload_purged"] == 1
              and rec7["acked_at"] is not None, "节点记录正文已持久清除，回执时间保留")

        print("\n== 场景 8：状态页/节点记录页/核实接口三处立即遮蔽，无原文泄漏 ==")
        _, st = http("GET", f"{proxy}/api/status")
        blob = json.dumps(st, ensure_ascii=False)
        check("TOP-SECRET-7" not in blob, "状态接口响应不含被清原文")
        _, rec = http("GET", f"{proxy}/api/nodes/{node7}/records")
        check("TOP-SECRET-7" not in json.dumps(rec), "节点记录接口不含被清原文")
        _, rfr = http("GET", f"{proxy}/api/writes/{pid7}/refresh")
        check("TOP-SECRET-7" not in json.dumps(rfr)
              and rfr["backend_view"]["payload"] == PURGE_MARKER,
              "核实接口不显露原文，只回清除标记")

        print("\n== 场景 9：节点故障 -> 待清除并自动重试，解除后后台对账完成 ==")
        _, w9 = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "TOP-SECRET-9"})
        pid9, node9 = w9["id"], w9["node"]
        http("POST", f"{proxy}/api/nodes/{node9}/fault", {"on": True})
        code, p9 = http("POST", f"{proxy}/api/writes/{pid9}/purge")
        check(code == 202 and p9["purge_state"] == "PURGE_PENDING" and p9["masked"],
              "节点失联时受理清除，返回 202 + PURGE_PENDING 且已遮蔽")
        # 即便后端不可达，代理侧记录页也立即遮蔽（不依赖后端）。
        row9 = find_write(proxy, pid9)
        check(row9["payload"] == PURGE_MARKER, "待清除期间代理状态页正文已遮蔽")
        time.sleep(1.6)
        pend = http("GET", f"{proxy}/api/status")[1]["purges"]
        still = any(x["write_id"] == pid9 and x["attempts"] >= 1 for x in pend["pending"])
        check(still, "故障期间多次重试仍保持待清除（attempts 增长）")
        http("POST", f"{proxy}/api/nodes/{node9}/fault", {"on": False})
        done9 = wait_purge_state(proxy, pid9, "PURGE_DONE", timeout=12)
        check(done9["target_node"] == node9, "故障解除后后台对账自动完成，归属不变")
        check(find_record(proxy, node9, pid9)["payload"] == PURGE_MARKER,
              "完成后节点 SQLite 正文为清除标记")

        print("\n== 场景 10：代理重启 -> 待清除单恢复接续，重启后仍遮蔽 ==")
        _, w10 = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "TOP-SECRET-10"})
        pid10, node10 = w10["id"], w10["node"]
        http("POST", f"{proxy}/api/nodes/{node10}/fault", {"on": True})
        http("POST", f"{proxy}/api/writes/{pid10}/purge")
        check(find_write(proxy, pid10)["payload"] == PURGE_MARKER, "重启前代理侧已遮蔽")
        stop_proxy()
        # 重启一个全新代理进程，复用同一 SQLite。
        spawn_proxy()
        check(wait_ready(proxy + "/"), "代理重启成功")
        _, st = http("GET", f"{proxy}/api/status")
        check(any(x["write_id"] == pid10 and x["purge_state"] == "PURGE_PENDING"
                  for x in st["purges"]["pending"]), "重启后从 SQLite 恢复待清除单")
        check(find_write(proxy, pid10)["payload"] == PURGE_MARKER,
              "重启后正文遮蔽依旧（持久化生效）")
        http("POST", f"{proxy}/api/nodes/{node10}/fault", {"on": False})
        wait_purge_state(proxy, pid10, "PURGE_DONE", timeout=12)
        check(find_record(proxy, node10, pid10)["payload"] == PURGE_MARKER,
              "重启恢复后最终清除节点正文")

        print("\n== 场景 11：清除与在途写入交错（先清后到），正文不重新写入 ==")
        # 发起一个慢写入（1.2s < 读超时 2s），拿到其 ID 后立刻清除（后端行可能尚未可清）。
        slow11 = SlowPost(proxy, 1.2, "TOP-SECRET-11")
        slow11.start()
        pid11 = None
        dl = time.time() + 5
        while time.time() < dl and pid11 is None:
            _, s = http("GET", f"{proxy}/api/status")
            for w in s["writes"]:
                if w["status"] == "PENDING":
                    pid11 = w["id"]
                    node11 = w["target_node"]
            time.sleep(0.1)
        check(bool(pid11), "慢写入已进入 PENDING，取得其 ID")
        code, p11 = http("POST", f"{proxy}/api/writes/{pid11}/purge")
        check(p11["masked"] and p11["write"]["payload"] == PURGE_MARKER,
              "在途写入被受理清除，代理侧立即遮蔽")
        slow11.join(timeout=12)
        check(slow11.result and slow11.result[0] in (201, 504),
              "慢写入投递未被清除打断，请求正常落定（拿到回执或显式不确定）")
        wait_purge_state(proxy, pid11, "PURGE_DONE", timeout=12)
        r11 = find_record(proxy, node11, pid11)
        check(r11["payload"] == PURGE_MARKER and "TOP-SECRET-11" not in json.dumps(r11),
              "写入落库后清除单接续完成，正文从未在任何接口显露")
        wait_status(proxy, pid11, "ACKED", timeout=8)
        check(find_write(proxy, pid11)["status"] == "ACKED", "回执仍正常保留为 ACKED")

        print("\n== 场景 12：清除与慢回执交错，正文不恢复，回执仍可向原节点核实 ==")
        slow12 = SlowPost(proxy, 1.2, "TOP-SECRET-12")
        slow12.start()
        pid12 = None
        dl = time.time() + 5
        while time.time() < dl and pid12 is None:
            _, s = http("GET", f"{proxy}/api/status")
            for w in s["writes"]:
                if w["status"] == "PENDING":
                    pid12, node12 = w["id"], w["target_node"]
            time.sleep(0.1)
        # 等后端已落库（行存在）但仍在慢回执窗口内时清除。
        time.sleep(0.4)
        code, p12 = http("POST", f"{proxy}/api/writes/{pid12}/purge")
        check(p12["purge_state"] == "PURGE_DONE", "慢回执窗口内对已落库行清除成功")
        slow12.join(timeout=12)
        check(slow12.result and slow12.result[0] in (201, 504),
              "慢回执最终正常返回（未被清除打断）")
        r12 = find_record(proxy, node12, pid12)
        check(r12["payload"] == PURGE_MARKER and r12["acked_at"] is not None,
              "慢回执更新 acked_at 但未恢复正文，回执可向原节点核实")
        _, rf12 = http("GET", f"{proxy}/api/writes/{pid12}/refresh")
        check(rf12["write"]["status"] == "ACKED"
              and rf12["backend_view"]["payload"] == PURGE_MARKER,
              "核实接口：状态 ACKED 且正文保持遮蔽")

        print("\n== 场景 13：切流不改变清除归属；重复清除幂等 ==")
        _, st = http("GET", f"{proxy}/api/status")
        cur_active = st["active"]
        other = "B" if cur_active == "A" else "A"
        code, swx = http("POST", f"{proxy}/api/switch", {"target": other})
        check(code == 200 and swx["active"] == other, f"切流到 {other} 成功")
        # 对一张已完成的旧清除单重复操作。
        code, again = http("POST", f"{proxy}/api/writes/{pid7}/purge")
        check(code == 200 and again["purge_state"] == "PURGE_DONE"
              and again["node"] == node7, "重复清除幂等，仍绑定原节点、不随切流改变")
        # 新写入落新节点，其清除单绑定新节点；切回再核对归属。
        _, w13 = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "TOP-SECRET-13"})
        pid13 = w13["id"]
        http("POST", f"{proxy}/api/writes/{pid13}/purge")
        p13 = wait_purge_state(proxy, pid13, "PURGE_DONE")
        check(p13["target_node"] == other, "新写入清除单归属当前现役节点（切流后正确绑定）")

        print("\n== 场景 14：直连两侧 SQLite 核对，被清行无原文残留 ==")
        for node, db_path in (("A", work / "a.db"), ("B", work / "b.db")):
            con = sqlite3.connect(db_path)
            con.row_factory = sqlite3.Row
            rows = con.execute(
                "SELECT id, payload, payload_purged FROM writes").fetchall()
            leaked = [r["id"] for r in rows
                      if "TOP-SECRET" in (r["payload"] or "")]
            purged_rows = [r for r in rows if r["payload_purged"] == 1]
            good = all(r["payload"] == PURGE_MARKER for r in purged_rows) and not leaked
            check(good, f"后端 {node} SQLite：{len(purged_rows)} 行已清且无原文残留",
                  f"leaked={len(leaked)}")
            con.close()
        pcon = sqlite3.connect(proxy_db)
        pcon.row_factory = sqlite3.Row
        prow = pcon.execute("SELECT payload, payload_purged FROM writes WHERE id=?",
                            (pid7,)).fetchone()
        check(prow["payload"] == PURGE_MARKER and prow["payload_purged"] == 1,
              "代理 SQLite：被清行正文为标记、遮蔽已持久化")
        ticket = pcon.execute(
            "SELECT purge_state, target_node FROM purges WHERE write_id=?",
            (pid7,)).fetchone()
        check(ticket["purge_state"] == "PURGE_DONE" and ticket["target_node"] == node7,
              "代理 SQLite：清除单完成且归属正确")
        pcon.close()

        print("\n== 场景 6：全量核对，两侧后端写入不相交、每条恰好一次 ==")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(ids_a.isdisjoint(ids_b), "A/B 后端写入 ID 集合不相交（无任何重复投递）")
        _, s_all = http("GET", f"{proxy}/api/status")
        proxy_ids = {w["id"] for w in s_all["writes"]}
        check(proxy_ids == ids_a | ids_b,
              f"代理侧 {len(proxy_ids)} 条写入与后端记录一一对应",
              f"A={len(ids_a)} B={len(ids_b)}")
        for node, db_path in (("A", work / "a.db"), ("B", work / "b.db")):
            con = sqlite3.connect(db_path)
            n = con.execute("SELECT COUNT(*) FROM writes").fetchone()[0]
            con.close()
            check(n == len(ids_a if node == "A" else ids_b),
                  f"后端 {node} 的 SQLite 持久化写入数 = {n}")

    finally:
        print("\n== 清理进程 ==")
        stop_proxy()
        for p in backends:
            p.send_signal(signal.SIGTERM)
        for p in backends:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
        if FAIL:
            print(f"\n日志保留在: {logs}")
        else:
            shutil.rmtree(work, ignore_errors=True)

    print(f"\n================ 自测结果: {PASS} 通过 / {FAIL} 失败 ================")
    if FAIL:
        for ok, line in RESULTS:
            if not ok:
                print("  " + line)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
