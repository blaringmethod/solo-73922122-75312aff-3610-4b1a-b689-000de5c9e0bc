"""SQLite 持久层：目录与摘要（不存原件字节）。

并发模型
--------
* WAL 模式 + ``busy_timeout``：两个实例共享同一库文件，读者不阻塞写者；
* 发布事务使用 ``BEGIN IMMEDIATE``，跨实例写操作被数据库锁串行化，
  后到者阻塞等待后重检唯一键，因此不会分叉、不会互相覆盖。
* 无任何 UPDATE/DELETE 路径，发布后记录不可改、不可删。

目录库的打开与初始化（防"丢库后静默从 1 号重发"）
------------------------------------------------
* 运行期连接一律使用 ``mode=rw`` URI 打开：库文件缺失时直接报错，
  **任何查询都不会在卷上补建空库**。
* 启动时在 ``.init.lock`` 文件锁内判定卷状态（双实例同时启动只会串行进入）：

  - 库文件存在且模式完整（三张表 + ``schema_version`` 标记 + 零号检查点）
    -> 正常服务；
  - 库文件缺失/空库/半初始化，且卷上**留有原件** -> 判定目录故障：
    不创建、不修改、不删除任何文件，实例以 503 拒服并在 /health 报告；
  - 库文件缺失/半初始化，且卷上**无任何原件** -> 视为全新卷或可重试的
    首次建库：半初始化文件改名留证后，在临时文件中建好完整模式，
    再 ``os.replace`` 原子生效。建库被中断只会留下 ``.release.db.init-*``
    临时文件，下次启动自动清理并重试，半初始化库绝不会用于发布或供数。
"""
from __future__ import annotations

import fcntl
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
from urllib.parse import quote
from uuid import uuid4

from . import merkle
from .config import Settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS releases (
    id           INTEGER PRIMARY KEY,          -- 1 起稠密序号，叶子下标 = id - 1
    station_id   TEXT NOT NULL,
    sample_date  TEXT NOT NULL,
    order_no     TEXT NOT NULL,
    blob_sha256  TEXT NOT NULL,
    size         INTEGER NOT NULL,
    created_at   TEXT NOT NULL,
    UNIQUE(station_id, sample_date, order_no)
);

CREATE TABLE IF NOT EXISTS checkpoints (
    seq         INTEGER PRIMARY KEY,            -- 该检查点对应的条数
    root_hex    TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
"""

SCHEMA_VERSION = "1"
REQUIRED_TABLES = {"releases", "checkpoints", "meta"}


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class ConflictError(Exception):
    """业务冲突：异输入重试或超出累计上限。"""


class DirectoryFaultError(Exception):
    """目录库缺失或未完整初始化且禁止自动重建：一切读写拒绝服务。"""


class Database:
    def __init__(self, settings: Settings):
        self._settings = settings
        settings.ensure_dirs()
        self._fault: str | None = None
        self._open_or_initialize()

    # ------------------------------------------------------------------ #
    # 启动：打开或（仅在全新卷上）初始化目录库
    # ------------------------------------------------------------------ #
    @property
    def fault(self) -> str | None:
        """启动时判定的目录故障说明；None 表示目录库完整可用。"""
        return self._fault

    def runtime_fault(self) -> str | None:
        """运行期目录故障自检（每次 /health 调用一次，开销为一次 stat）。"""
        if self._fault is not None:
            return self._fault
        if not self._settings.db_path.exists():
            return (
                f"目录库 {self._settings.db_path.name} 在运行期从卷上消失，"
                "拒绝补建，请恢复原目录库并重启实例"
            )
        return None

    def _open_or_initialize(self) -> None:
        # 双实例同时启动全新卷时，建库判定在文件锁内串行进行。
        lock_path = self._settings.data_dir / ".init.lock"
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            self._open_or_initialize_locked()
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _open_or_initialize_locked(self) -> None:
        db_path = self._settings.db_path
        if db_path.exists():
            problem = self._probe_schema(db_path)
            if problem is None:
                return  # 完整目录库：正常服务（孤立原件无害）。
            if self._volume_has_originals():
                self._fault = (
                    f"目录库 {db_path.name} 未完整初始化（{problem}），且卷上留有"
                    "原件：疑似目录丢失，拒绝自动重建。请恢复原目录库后重启实例。"
                )
                return
            # 全新卷上半初始化的库（如首次建库被中断的残留）：改名留证后重建。
            self._quarantine_half_initialized(db_path, problem)
            self._initialize_fresh()
            return
        if self._volume_has_originals():
            self._fault = (
                f"目录库 {db_path.name} 缺失，但卷上留有原件：疑似目录丢失，"
                "拒绝自动重建。请恢复原目录库后重启实例。"
            )
            return
        self._initialize_fresh()

    @staticmethod
    def _probe_schema(db_path: Path) -> str | None:
        """检查库文件是否为完整初始化的目录库；返回 None 表示完整。"""
        try:
            conn = sqlite3.connect(
                f"file:{quote(str(db_path))}?mode=rw",
                uri=True,
                timeout=15.0,
            )
        except sqlite3.Error as exc:
            return f"无法打开（{exc}）"
        try:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            missing = sorted(REQUIRED_TABLES - tables)
            if missing:
                return f"缺少数据表 {missing}"
            meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
            if meta.get("schema_version") != SCHEMA_VERSION:
                return "缺少 schema_version 标记或版本不符"
            row = conn.execute(
                "SELECT 1 FROM checkpoints WHERE seq = 0"
            ).fetchone()
            if row is None:
                return "缺少零号检查点"
            return None
        except sqlite3.Error as exc:
            return f"不是可用的 SQLite 目录库（{exc}）"
        finally:
            conn.close()

    def _volume_has_originals(self) -> bool:
        """卷上是否留有原件（内容寻址 blob 文件）。临时目录残留不算。"""
        blob_dir = self._settings.blob_dir
        if not blob_dir.is_dir():
            return False
        for _root, _dirs, files in os.walk(blob_dir):
            if files:
                return True
        return False

    def _quarantine_half_initialized(self, db_path: Path, problem: str) -> None:
        """半初始化且无原件：把残库改名留在卷上备查，不删除任何数据。"""
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        target = db_path.with_name(
            f"{db_path.name}.half-init-{stamp}-{os.getpid()}"
        )
        os.replace(db_path, target)
        for suffix in ("-wal", "-shm"):
            side = db_path.with_name(db_path.name + suffix)
            if side.exists():
                os.replace(side, target.with_name(target.name + suffix))
        _fsync_dir(db_path.parent)

    def _initialize_fresh(self) -> None:
        """全新卷建库：临时文件建完整模式 -> 原子改名生效，可安全重试。"""
        db_path = self._settings.db_path
        # 清理以往中断建库留下的临时文件（仅在确认无原件的全新卷上执行，
        # 且处于文件锁内，不会误删另一实例正在使用的临时库）。
        for stale in db_path.parent.glob(f".{db_path.name}.init-*"):
            stale.unlink(missing_ok=True)
        tmp_path = db_path.with_name(
            f".{db_path.name}.init-{os.getpid()}-{uuid4().hex[:8]}"
        )
        conn = sqlite3.connect(tmp_path, timeout=15.0, isolation_level=None)
        try:
            # 回滚日志模式：提交并关闭后全部内容都在单一文件内，
            # 之后的原子改名即"建库"这唯一的对外可见动作。
            conn.execute("PRAGMA journal_mode = DELETE")
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executescript(SCHEMA)
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES ('schema_version', ?)",
                    (SCHEMA_VERSION,),
                )
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES ('created_at', ?)",
                    (_utcnow(),),
                )
                # 零号检查点（空树根）。
                empty = merkle.empty_root(self._settings.tree_depth).hex()
                conn.execute(
                    "INSERT INTO checkpoints(seq, root_hex, created_at) "
                    "VALUES (0, ?, ?)",
                    (empty, _utcnow()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        finally:
            conn.close()
        os.replace(tmp_path, db_path)
        _fsync_dir(db_path.parent)

    # ------------------------------------------------------------------ #
    # 运行期连接：绝不补建库文件
    # ------------------------------------------------------------------ #
    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        if self._fault is not None:
            raise DirectoryFaultError(self._fault)
        db_path = self._settings.db_path
        if not db_path.exists():
            raise DirectoryFaultError(
                f"目录库 {db_path.name} 在运行期从卷上消失，拒绝补建，"
                "请恢复原目录库并重启实例"
            )
        conn = sqlite3.connect(
            f"file:{quote(str(db_path))}?mode=rw",  # 缺失即报错，绝不创建
            uri=True,
            timeout=15.0,
            isolation_level=None,  # 手动事务，显式 BEGIN/COMMIT
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = 15000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    # 发布
    # ------------------------------------------------------------------ #
    def publish(
        self,
        station_id: str,
        sample_date: str,
        order_no: str,
        blob_sha256: str,
        size: int,
    ) -> tuple[dict, bool]:
        """串行化发布。返回 (发布记录, 是否复用既有记录)。

        调用方必须已把原件原子落盘成功后再调用本方法，
        从而保证“库中有记录 => 卷上有完整原件”。
        """
        # 双实例并发时 BEGIN IMMEDIATE 可能撞上写锁；busy_timeout 之外再有限重试。
        for attempt in range(3):
            try:
                return self._publish_once(
                    station_id, sample_date, order_no, blob_sha256, size
                )
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() and attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                raise
        raise RuntimeError("unreachable")  # pragma: no cover

    def _publish_once(
        self,
        station_id: str,
        sample_date: str,
        order_no: str,
        blob_sha256: str,
        size: int,
    ) -> tuple[dict, bool]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM releases "
                    "WHERE station_id = ? AND sample_date = ? AND order_no = ?",
                    (station_id, sample_date, order_no),
                ).fetchone()
                if row is not None:
                    record = dict(row)
                    same = (
                        record["blob_sha256"] == blob_sha256
                        and record["size"] == size
                    )
                    conn.rollback()  # 只读复用，不产生写入
                    if not same:
                        raise ConflictError(
                            "同一站号/日期/发布单号已存在且原件内容不同，禁止改写"
                        )
                    return record, True

                count = conn.execute(
                    "SELECT COUNT(*) AS c FROM releases"
                ).fetchone()["c"]
                if count >= self._settings.max_releases:
                    conn.rollback()
                    raise ConflictError(
                        f"累计发布已达上限 {self._settings.max_releases} 件"
                    )

                cursor = conn.execute(
                    "INSERT INTO releases(station_id, sample_date, order_no, "
                    "blob_sha256, size, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        station_id,
                        sample_date,
                        order_no,
                        blob_sha256,
                        size,
                        _utcnow(),
                    ),
                )
                release_id = cursor.lastrowid
                # 无删除路径，rowid 必然稠密且等于新条数。
                assert release_id == count + 1

                leaves = self._leaf_hashes_locked(conn)
                root = merkle.root_from_leaves(leaves, self._settings.tree_depth)
                conn.execute(
                    "INSERT INTO checkpoints(seq, root_hex, created_at) "
                    "VALUES (?, ?, ?)",
                    (len(leaves), root.hex(), _utcnow()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        record = self.get_release_by_id(release_id)
        return record, False

    @staticmethod
    def _leaf_hashes_locked(conn: sqlite3.Connection) -> list[bytes]:
        rows = conn.execute(
            "SELECT station_id, sample_date, order_no, blob_sha256, size "
            "FROM releases ORDER BY id ASC"
        ).fetchall()
        return [
            merkle.leaf_hash(
                r["station_id"],
                r["sample_date"],
                r["order_no"],
                r["blob_sha256"],
                r["size"],
            )
            for r in rows
        ]

    def leaf_hashes(self) -> list[bytes]:
        with self.connect() as conn:
            return self._leaf_hashes_locked(conn)

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def get_release(self, station_id: str, sample_date: str, order_no: str):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE station_id = ? AND sample_date = ? "
                "AND order_no = ?",
                (station_id, sample_date, order_no),
            ).fetchone()
            return dict(row) if row else None

    def get_release_by_id(self, release_id: int):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE id = ?", (release_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_releases(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM releases ORDER BY id ASC").fetchall()
            return [dict(r) for r in rows]

    def latest_checkpoint(self) -> dict:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            return dict(row)

    def get_checkpoint(self, seq: int):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints WHERE seq = ?", (seq,)
            ).fetchone()
            return dict(row) if row else None

    def list_checkpoints(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq ASC"
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # 启动自检：不得带着缺件记录对外服务
    # ------------------------------------------------------------------ #
    def integrity_problems(self, blob_verifier) -> list[str]:
        problems: list[str] = []
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id, blob_sha256 FROM releases ORDER BY id"
            ).fetchall()
            expected_ids = list(range(1, len(rows) + 1))
            actual_ids = [r["id"] for r in rows]
            if actual_ids != expected_ids:
                problems.append("releases 主键不稠密，存在缺口或乱序")
            for row in rows:
                if not blob_verifier(row["blob_sha256"]):
                    problems.append(
                        f"release id={row['id']} 的原件缺失或摘要不符 "
                        f"(sha256={row['blob_sha256'][:12]}...)"
                    )
            leaves = self._leaf_hashes_locked(conn)
            root = merkle.root_from_leaves(leaves, self._settings.tree_depth)
            cp_rows = conn.execute(
                "SELECT seq, root_hex FROM checkpoints ORDER BY seq"
            ).fetchall()
            cp_seqs = [r["seq"] for r in cp_rows]
            if cp_seqs != list(range(len(cp_seqs))):
                problems.append("checkpoints 序号不连续")
            latest = cp_rows[-1]
            if latest["seq"] != len(leaves) or latest["root_hex"] != root.hex():
                problems.append("最新检查点与实际叶子集合不一致")
        return problems
