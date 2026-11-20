"""目录库打开与初始化：丢库/空库/半初始化/并发首建/恢复续发。

覆盖需求：
* 有原件的卷 + 目录库缺失或仅剩未建表的空库 -> 目录故障，任何查询不得补建；
* 恢复原目录库后按原检查点续发；
* 全新卷可并发首建，首次建库中断可重试，半初始化库不可用于发布或供数；
* 已有完整目录的旧卷即使留有未登记原件也照常使用。
"""
from __future__ import annotations

import io
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from app import main as main_mod
from app.config import Settings
from app.db import Database, DirectoryFaultError


def _settings(tmp_path: Path, name: str = "vol") -> Settings:
    data_dir = tmp_path / name
    return Settings(
        data_dir=data_dir,
        db_path=data_dir / "release.db",
        blob_dir=data_dir / "blobs",
        temp_dir=data_dir / "tmp",
        max_file_bytes=64 * 1024,
        max_releases=16,
        tree_depth=4,
    )


def _publish_row(db: Database, station: str, order: str, data: bytes) -> dict:
    """模拟 main 流程：先落原件，再入目录库。"""
    from app.storage import BlobStore

    store = BlobStore(db._settings)
    digest, _ = store.store(data)
    record, _ = db.publish(station, "2026-11-16", order, digest, len(data))
    return record


def _db_side_files(db_path: Path) -> list[Path]:
    return [p for p in db_path.parent.glob(db_path.name + "*") if p.is_file()]


# --------------------------------------------------------------------------- #
# 全新卷：建库一次成功、可并发、可重试
# --------------------------------------------------------------------------- #
def test_fresh_volume_initializes_and_reopens(tmp_path):
    settings = _settings(tmp_path)
    db = Database(settings)
    assert db.fault is None
    assert settings.db_path.exists()
    # 完整初始化标记：三张表 + schema_version + 零号检查点。
    probe = Database._probe_schema(settings.db_path)
    assert probe is None
    assert db.latest_checkpoint()["seq"] == 0

    rec = _publish_row(db, "ST1", "ORD-1", b"a,ph\nS1,7.0\n")
    assert rec["id"] == 1

    # 重新打开（模拟重启）：不重建、序号连续。
    db2 = Database(settings)
    assert db2.fault is None
    rec2 = _publish_row(db2, "ST2", "ORD-2", b"a,ph\nS2,7.1\n")
    assert rec2["id"] == 2
    assert db2.latest_checkpoint()["seq"] == 2


def test_concurrent_first_init_on_fresh_volume(tmp_path):
    """两个实例同时启动全新卷：文件锁串行，恰建库一次，双双健康。"""
    settings = _settings(tmp_path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        dbs = list(pool.map(lambda _: Database(settings), range(2)))
    assert all(db.fault is None for db in dbs)
    assert settings.db_path.exists()
    # 只有一个零号检查点，没有重复建库痕迹。
    cps = dbs[0].list_checkpoints()
    assert [c["seq"] for c in cps] == [0]
    # 两个实例交替发布，序号稠密。
    r1 = _publish_row(dbs[0], "STA", "ORD-A", b"x\n")
    r2 = _publish_row(dbs[1], "STB", "ORD-B", b"y\n")
    assert sorted([r1["id"], r2["id"]]) == [1, 2]


def test_interrupted_first_init_is_retried(tmp_path):
    """首次建库中断：只留下临时文件，重启后自动清理并重试成功。"""
    settings = _settings(tmp_path)
    settings.ensure_dirs()
    # 模拟建库进程在原子改名前崩溃：残留临时库文件，没有 release.db。
    stale = settings.data_dir / ".release.db.init-9999-deadbeef"
    stale.write_bytes(b"sqlite-partial")
    db = Database(settings)
    assert db.fault is None
    assert settings.db_path.exists()
    assert not stale.exists()  # 残留临时文件已清理
    rec = _publish_row(db, "ST1", "ORD-1", b"z\n")
    assert rec["id"] == 1


def test_half_initialized_db_without_originals_quarantined_and_retried(tmp_path):
    """半初始化库（缺表）+ 无原件：不可用于发布/供数，改名留证后重建。"""
    settings = _settings(tmp_path)
    settings.ensure_dirs()
    # 手工制造半初始化库：只有 releases 表，缺 meta/checkpoints。
    conn = sqlite3.connect(settings.db_path)
    conn.execute("CREATE TABLE releases (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    db = Database(settings)
    assert db.fault is None  # 重建成功，可正常服务
    assert Database._probe_schema(settings.db_path) is None
    rec = _publish_row(db, "ST1", "ORD-1", b"q\n")
    assert rec["id"] == 1
    # 半初始化残库被保留在卷上备查，未被删除。
    leftovers = list(settings.data_dir.glob("release.db.half-init-*"))
    assert len(leftovers) == 1


# --------------------------------------------------------------------------- #
# 有原件的卷 + 目录缺失/空库/半初始化 -> 目录故障
# --------------------------------------------------------------------------- #
def test_missing_db_with_originals_is_fault_and_creates_nothing(tmp_path):
    settings = _settings(tmp_path)
    db = Database(settings)
    _publish_row(db, "ST1", "ORD-1", b"keep-me\n")
    _publish_row(db, "ST2", "ORD-2", b"keep-me-too\n")
    blob_snapshot = sorted(
        p.relative_to(settings.blob_dir) for p in settings.blob_dir.rglob("*") if p.is_file()
    )
    assert blob_snapshot, "前置条件：卷上应有原件"

    # 模拟目录库丢失（保留全部原件）。
    for path in _db_side_files(settings.db_path):
        path.unlink()
    assert not settings.db_path.exists()

    db2 = Database(settings)
    assert db2.fault is not None and "缺失" in db2.fault
    # 任何触库操作都抛 DirectoryFaultError，且绝不补建库文件。
    with pytest.raises(DirectoryFaultError):
        db2.list_releases()
    with pytest.raises(DirectoryFaultError):
        db2.latest_checkpoint()
    with pytest.raises(DirectoryFaultError):
        db2.publish("ST3", "2026-11-16", "ORD-3", "0" * 64, 1)
    assert not settings.db_path.exists()
    assert _db_side_files(settings.db_path) == []
    # 原件原样保留。
    assert sorted(
        p.relative_to(settings.blob_dir) for p in settings.blob_dir.rglob("*") if p.is_file()
    ) == blob_snapshot


def test_empty_db_file_with_originals_is_fault(tmp_path):
    """仅剩未建表的空库（0 字节）+ 卷上有原件 -> 目录故障，不重建。"""
    settings = _settings(tmp_path)
    db = Database(settings)
    _publish_row(db, "ST1", "ORD-1", b"blob-bytes\n")
    for path in _db_side_files(settings.db_path):
        path.unlink()
    settings.db_path.write_bytes(b"")  # 查询误建的 0 字节空库

    db2 = Database(settings)
    assert db2.fault is not None and "未完整初始化" in db2.fault
    # 空库文件保持原样，未被重建或删除。
    assert settings.db_path.exists()
    assert settings.db_path.read_bytes() == b""
    with pytest.raises(DirectoryFaultError):
        db2.list_releases()


def test_tableless_sqlite_db_with_originals_is_fault(tmp_path):
    """合法 SQLite 但无任何业务表 + 卷上有原件 -> 目录故障。"""
    settings = _settings(tmp_path)
    db = Database(settings)
    _publish_row(db, "ST1", "ORD-1", b"blob-bytes\n")
    for path in _db_side_files(settings.db_path):
        path.unlink()
    conn = sqlite3.connect(settings.db_path)  # 合法但空的 SQLite 库
    conn.execute("CREATE TABLE keepalive (x)")
    conn.commit()
    conn.close()

    db2 = Database(settings)
    assert db2.fault is not None
    with pytest.raises(DirectoryFaultError):
        db2.publish("ST9", "2026-11-16", "ORD-9", "0" * 64, 1)


def test_half_initialized_db_with_originals_is_fault_and_untouched(tmp_path):
    """半初始化库 + 卷上有原件 -> 目录故障；残库保持原样留证。"""
    settings = _settings(tmp_path)
    db = Database(settings)
    _publish_row(db, "ST1", "ORD-1", b"blob-bytes\n")
    for path in _db_side_files(settings.db_path):
        path.unlink()
    conn = sqlite3.connect(settings.db_path)
    conn.execute("CREATE TABLE releases (id INTEGER PRIMARY KEY)")  # 缺 meta/checkpoints
    conn.commit()
    conn.close()
    before = settings.db_path.read_bytes()

    db2 = Database(settings)
    assert db2.fault is not None and "未完整初始化" in db2.fault
    assert settings.db_path.read_bytes() == before  # 未被改动
    assert list(settings.data_dir.glob("release.db.half-init-*")) == []


def test_complete_db_with_orphan_blobs_works(tmp_path):
    """已有完整目录的旧卷留有未登记原件：照常使用。"""
    settings = _settings(tmp_path)
    db = Database(settings)
    _publish_row(db, "ST1", "ORD-1", b"registered\n")
    # 未登记原件（孤立 blob）。
    orphan = settings.blob_dir / "ab" / "cd" / ("ab" * 32)
    orphan.parent.mkdir(parents=True, exist_ok=True)
    orphan.write_bytes(b"unregistered")

    db2 = Database(settings)
    assert db2.fault is None
    rec = _publish_row(db2, "ST2", "ORD-2", b"second\n")
    assert rec["id"] == 2


# --------------------------------------------------------------------------- #
# 恢复原目录库 -> 按原检查点续发
# --------------------------------------------------------------------------- #
def test_restore_original_db_resumes_from_checkpoint(tmp_path):
    settings = _settings(tmp_path)
    db = Database(settings)
    _publish_row(db, "ST1", "ORD-1", b"one\n")
    _publish_row(db, "ST2", "ORD-2", b"two\n")
    latest_before = db.latest_checkpoint()
    assert latest_before["seq"] == 2

    # 备份原目录库 -> 模拟丢失 -> 故障 -> 恢复。
    backup = tmp_path / "backup" / "release.db"
    backup.parent.mkdir()
    with db.connect() as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    backup.write_bytes(settings.db_path.read_bytes())
    for path in _db_side_files(settings.db_path):
        path.unlink()
    faulty = Database(settings)
    assert faulty.fault is not None

    settings.db_path.write_bytes(backup.read_bytes())
    resumed = Database(settings)
    assert resumed.fault is None
    assert resumed.latest_checkpoint()["seq"] == 2
    assert resumed.latest_checkpoint()["root_hex"] == latest_before["root_hex"]
    rec = _publish_row(resumed, "ST3", "ORD-3", b"three\n")
    assert rec["id"] == 3  # 从原检查点续发，而不是从 1 号重发
    assert resumed.latest_checkpoint()["seq"] == 3
    assert resumed.integrity_problems(
        lambda digest: True  # 目录层自检：此处只验证序号/检查点连续性
    ) == []


# --------------------------------------------------------------------------- #
# 运行期丢库：不补建、报故障
# --------------------------------------------------------------------------- #
def test_runtime_db_loss_never_recreates_file(tmp_path):
    settings = _settings(tmp_path)
    db = Database(settings)
    _publish_row(db, "ST1", "ORD-1", b"one\n")
    with db.connect() as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    for path in _db_side_files(settings.db_path):
        path.unlink()

    assert db.runtime_fault() is not None
    with pytest.raises(DirectoryFaultError):
        with db.connect():
            pass
    with pytest.raises(DirectoryFaultError):
        db.list_releases()
    assert not settings.db_path.exists()
    assert _db_side_files(settings.db_path) == []


# --------------------------------------------------------------------------- #
# 应用层接线：目录故障时全部数据面 503，/health 报告 directory_fault
# --------------------------------------------------------------------------- #
def _publish(client, station, day, order, data):
    return client.post(
        "/releases",
        data={"station_id": station, "sample_date": day, "order_no": order},
        files={"file": ("day.csv", io.BytesIO(data), "text/csv")},
    )


def test_directory_fault_returns_503_on_all_data_endpoints(app_client, monkeypatch):
    client, _app, _settings = app_client
    fault = "目录库 release.db 缺失，但卷上留有原件：疑似目录丢失"
    monkeypatch.setattr(main_mod, "_directory_fault", fault)
    monkeypatch.setattr(main_mod, "_startup_problems", [fault])

    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "directory_fault"
    assert body["release_count"] is None
    assert any("目录库" in p for p in body["problems"])

    # 发布、目录查询、检查点、取件、取证：一律 503。
    assert _publish(client, "STX", "2026-11-16", "ORD-X", b"x\n").status_code == 503
    assert client.get("/releases").status_code == 503
    assert client.get("/releases/ST1/2026-11-16/ORD-1").status_code == 503
    assert client.get("/checkpoints").status_code == 503
    assert client.get("/checkpoints/latest").status_code == 503
    assert client.get("/checkpoints/0").status_code == 503
    assert client.get("/checkpoints/0/append-proof").status_code == 503
    assert client.get("/releases/ST1/2026-11-16/ORD-1/download").status_code == 503
    assert (
        client.get("/releases/ST1/2026-11-16/ORD-1/inclusion-proof").status_code
        == 503
    )


def test_directory_fault_error_from_db_layer_maps_to_503(app_client, monkeypatch):
    """触库路径抛出 DirectoryFaultError 时（如运行期丢库）统一 503。"""
    client, _app, _settings = app_client

    def boom(self):
        raise DirectoryFaultError("模拟运行期目录库丢失")

    monkeypatch.setattr(main_mod.Database, "connect", boom)
    assert client.get("/releases").status_code == 503
    assert client.get("/checkpoints/latest").status_code == 503
