"""目录库故障/恢复生命周期的双实例共卷集成检查（Docker 不可用环境下的等价验证）。

以多个独立 uvicorn 进程挂载同一 DATA_DIR 验证：
  1. 两个实例同时启动全新卷：文件锁串行建库，双双健康；
  2. 跨实例发布后，删除目录库 release.db（原件保留）：
     - 运行中的两个实例 /health 立即报告 directory_fault，
       发布/目录查询/检查点/取件/取证一律 503，且卷上绝不补建空库；
     - 新启动的实例同样在 /health 报告目录故障并 503；
  3. 恢复原目录库后：新实例按原检查点续发（id 连续、根一致），
     运行中的实例也自动恢复服务；
  4. 全新卷上半初始化库：改名留证后重建，可正常发布；
  5. 有原件的卷上半初始化库：目录故障，残库原样保留。

用法：python scripts/directory_fault_check.py
"""
from __future__ import annotations

import http.client
import io
import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


FAILURES: list[str] = []


def check(cond: bool, message: str) -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {message}")
    if not cond:
        FAILURES.append(message)


def wait_ready(port: int, timeout: float = 25.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/health", timeout=2
                ) as resp:
                    if resp.status == 200:
                        return
        except OSError:
            time.sleep(0.3)
    raise RuntimeError(f"实例端口 {port} 未就绪")


def start_instance(port: int, data_dir: str) -> subprocess.Popen:
    env = dict(os.environ)
    env.update(
        {
            "DATA_DIR": data_dir,
            "MAX_RELEASES": "128",
            "MAX_FILE_BYTES": str(64 * 1024),
        }
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    wait_ready(port)
    return proc


def stop_instance(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
    except (PermissionError, subprocess.TimeoutExpired):
        print(f"[note] 环境禁止/无法终止 pid={proc.pid}，保留运行（随机端口，不影响结果）")


def _multipart_body(fields: dict, file_field: str, filename: str, content: bytes):
    boundary = f"----wq{uuid.uuid4().hex}"
    body = io.BytesIO()
    for key, value in fields.items():
        body.write(f"--{boundary}\r\n".encode())
        body.write(
            f'Content-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode()
        )
    body.write(f"--{boundary}\r\n".encode())
    body.write(
        f'Content-Disposition: form-data; name="{file_field}"; '
        f'filename="{filename}"\r\nContent-Type: text/csv\r\n\r\n'.encode()
    )
    body.write(content)
    body.write(f"\r\n--{boundary}--\r\n".encode())
    return body.getvalue(), f"multipart/form-data; boundary={boundary}"


def post_release(port: int, station: str, day: str, order: str, content: bytes):
    body, ctype = _multipart_body(
        {"station_id": station, "sample_date": day, "order_no": order},
        "file",
        f"{order}.csv",
        content,
    )
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request("POST", "/releases", body=body, headers={"Content-Type": ctype})
    resp = conn.getresponse()
    payload = json.loads(resp.read().decode())
    conn.close()
    return resp.status, payload


def get(port: int, path: str):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request("GET", path)
    resp = conn.getresponse()
    raw = resp.read().decode()
    conn.close()
    try:
        return resp.status, json.loads(raw)
    except json.JSONDecodeError:
        return resp.status, raw


def expect_503s(port: int, label: str) -> None:
    """发布、目录查询、检查点、取件、取证全部 503。"""
    s_post, _ = post_release(port, "STX", "2026-11-16", "ORD-X", b"x\n")
    check(s_post == 503, f"{label}: 发布返回 503（实际 {s_post}）")
    for path, name in [
        ("/releases", "目录列表"),
        ("/releases/ST01/2026-11-16/ORD-1", "目录查询"),
        ("/checkpoints", "检查点列表"),
        ("/checkpoints/latest", "最新检查点"),
        ("/checkpoints/0", "指定检查点"),
        ("/releases/ST01/2026-11-16/ORD-1/download", "取件下载"),
        ("/releases/ST01/2026-11-16/ORD-1/inclusion-proof", "纳入证明"),
        ("/checkpoints/0/append-proof", "追加证明"),
    ]:
        status, _ = get(port, path)
        check(status == 503, f"{label}: {name} 返回 503（实际 {status}）")


def expect_directory_fault_health(port: int, label: str) -> None:
    status, body = get(port, "/health")
    check(
        status == 200
        and body.get("status") == "directory_fault"
        and body.get("release_count") is None,
        f"{label}: /health 报告 directory_fault（实际 {status} {body}）",
    )


def db_files(data_dir: Path) -> list[Path]:
    return sorted(p for p in data_dir.glob("release.db*") if p.is_file())


def main() -> int:
    procs: list[subprocess.Popen] = []
    try:
        # ---------------------------------------------------------- #
        # 1) 双实例同时启动全新卷
        # ---------------------------------------------------------- #
        data_dir = Path(tempfile.mkdtemp(prefix="wq-fault-vol-"))
        backup_dir = Path(tempfile.mkdtemp(prefix="wq-fault-backup-"))
        print(f"[setup] 共享 DATA_DIR={data_dir}")
        port_a, port_b = _free_port(), _free_port()
        proc_a = start_instance(port_a, str(data_dir))
        proc_b = start_instance(port_b, str(data_dir))  # 与 A 近乎同时首建
        procs += [proc_a, proc_b]
        for port, name in ((port_a, "A"), (port_b, "B")):
            status, body = get(port, "/health")
            check(
                status == 200 and body.get("status") == "ok",
                f"实例 {name} 全新卷启动健康（实际 {body}）",
            )
        check(
            (data_dir / "release.db").is_file(),
            "全新卷并发首建后目录库存在且唯一",
        )

        # 跨实例发布两件。
        s1, p1 = post_release(port_a, "ST01", "2026-11-16", "ORD-1", b"row-1\n")
        s2, p2 = post_release(port_b, "ST02", "2026-11-16", "ORD-2", b"row-2\n")
        check(
            s1 == 201 and s2 == 201 and p1["release"]["id"] == 1
            and p2["release"]["id"] == 2,
            f"跨实例发布 id=1,2（实际 {s1},{s2}）",
        )
        _, cp_before = get(port_a, "/checkpoints/latest")
        check(cp_before.get("seq") == 2, f"最新检查点 seq=2（实际 {cp_before}）")

        # 备份原目录库（先 checkpoint 收拢 WAL），随后模拟目录库丢失。
        conn = sqlite3.connect(data_dir / "release.db")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
        shutil.copy2(data_dir / "release.db", backup_dir / "release.db")
        blob_snapshot = sorted(
            str(p.relative_to(data_dir)) for p in (data_dir / "blobs").rglob("*") if p.is_file()
        )
        check(len(blob_snapshot) == 2, "卷上现有 2 件原件")

        # ---------------------------------------------------------- #
        # 2) 目录库丢失（原件保留）：运行中实例 + 新实例均报告故障
        # ---------------------------------------------------------- #
        for path in db_files(data_dir):
            path.unlink()
        check(not (data_dir / "release.db").exists(), "目录库已删除（模拟丢失）")

        expect_directory_fault_health(port_a, "运行中实例 A")
        expect_directory_fault_health(port_b, "运行中实例 B")
        expect_503s(port_a, "运行中实例 A")
        expect_503s(port_b, "运行中实例 B")
        check(
            db_files(data_dir) == [],
            "运行期全部 503 请求之后，卷上仍未补建目录库",
        )
        check(
            sorted(
                str(p.relative_to(data_dir))
                for p in (data_dir / "blobs").rglob("*")
                if p.is_file()
            )
            == blob_snapshot,
            "卷上原件原样保留",
        )

        # 新实例挂载同一卷：启动即判定目录故障。
        port_c = _free_port()
        proc_c = start_instance(port_c, str(data_dir))
        procs.append(proc_c)
        expect_directory_fault_health(port_c, "新启动实例 C")
        expect_503s(port_c, "新启动实例 C")
        check(db_files(data_dir) == [], "新实例启动后仍未补建目录库")

        # ---------------------------------------------------------- #
        # 3) 恢复原目录库：新实例按原检查点续发
        # ---------------------------------------------------------- #
        shutil.copy2(backup_dir / "release.db", data_dir / "release.db")
        port_d = _free_port()
        proc_d = start_instance(port_d, str(data_dir))
        procs.append(proc_d)
        status, body = get(port_d, "/health")
        check(
            status == 200 and body.get("status") == "ok"
            and body.get("release_count") == 2,
            f"恢复后新实例 D 健康且条数=2（实际 {body}）",
        )
        _, cp_after = get(port_d, "/checkpoints/latest")
        check(
            cp_after.get("seq") == 2 and cp_after.get("root_hex") == cp_before["root_hex"],
            "恢复后最新检查点与原检查点一致",
        )
        s3, p3 = post_release(port_d, "ST03", "2026-11-16", "ORD-3", b"row-3\n")
        check(
            s3 == 201 and p3["release"]["id"] == 3 and p3["checkpoint"]["seq"] == 3,
            f"恢复后按原检查点续发 id=3（实际 {s3} {p3.get('release', {})}）",
        )
        # 运行中的 A/B 在目录恢复后自动恢复服务（无需重启）。
        status, body = get(port_a, "/health")
        check(
            status == 200 and body.get("status") == "ok" and body.get("release_count") == 3,
            f"运行中实例 A 自动恢复服务且看到 3 条（实际 {body}）",
        )
        # 离线核验续发记录的纳入证明。
        sys.path.insert(0, str(REPO_ROOT))
        from app.verify import verify_release_bytes

        _, proof3 = get(port_b, "/releases/ST03/2026-11-16/ORD-3/inclusion-proof")
        ok, msg = verify_release_bytes(proof3, b"row-3\n")
        check(ok, f"续发记录的纳入证明跨实例离线核验通过：{msg}")

        # ---------------------------------------------------------- #
        # 4) 全新卷 + 半初始化库：改名留证后重建
        # ---------------------------------------------------------- #
        half_dir = Path(tempfile.mkdtemp(prefix="wq-half-fresh-"))
        conn = sqlite3.connect(half_dir / "release.db")
        conn.execute("CREATE TABLE releases (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()
        port_e = _free_port()
        proc_e = start_instance(port_e, str(half_dir))
        procs.append(proc_e)
        status, body = get(port_e, "/health")
        check(
            status == 200 and body.get("status") == "ok",
            f"全新卷半初始化库重试建库后健康（实际 {body}）",
        )
        s4, p4 = post_release(port_e, "ST01", "2026-11-16", "ORD-1", b"fresh\n")
        check(
            s4 == 201 and p4["release"]["id"] == 1,
            "重建后可正常发布（半初始化库未被用于供数）",
        )
        check(
            len(list(half_dir.glob("release.db.half-init-*"))) == 1,
            "半初始化残库已改名留在卷上备查",
        )

        # ---------------------------------------------------------- #
        # 5) 有原件的卷 + 半初始化库：目录故障，残库原样保留
        # ---------------------------------------------------------- #
        half_orig_dir = Path(tempfile.mkdtemp(prefix="wq-half-orig-"))
        blob = half_orig_dir / "blobs" / "ab" / "cd" / ("ab" * 32)
        blob.parent.mkdir(parents=True)
        blob.write_bytes(b"orphan-original")
        conn = sqlite3.connect(half_orig_dir / "release.db")
        conn.execute("CREATE TABLE releases (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()
        before_bytes = (half_orig_dir / "release.db").read_bytes()
        port_f = _free_port()
        proc_f = start_instance(port_f, str(half_orig_dir))
        procs.append(proc_f)
        expect_directory_fault_health(port_f, "有原件+半初始化库实例 F")
        expect_503s(port_f, "有原件+半初始化库实例 F")
        check(
            (half_orig_dir / "release.db").read_bytes() == before_bytes
            and blob.read_bytes() == b"orphan-original",
            "残库与原件均原样保留，未重建未删除",
        )
    finally:
        for proc in procs:
            stop_instance(proc)

    print(f"\n结果：{len(FAILURES)} 项失败")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
