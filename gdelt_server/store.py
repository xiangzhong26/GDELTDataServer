"""Only sufficient statistics and a durable batch ledger are stored.

Each file contributes to BOTH hour and day buckets in one transaction.
Daily data is never reconstructed from potentially expired hourly data.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import copy
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import shutil
import sqlite3

HOUR, DAY, SLOT = 3600, 86400, 900
QUAD_DIRECTION = {1: .4, 2: 1., 3: -.4, 4: -1.}
MENTION_CAP = 20
REL_METRICS = ("n_events", "sum_mentions", "sum_sources", "sum_articles", "sum_w",
               "sum_gold_w", "sum_quad_w", "sum_tone_w", "sum_gold", "sum_tone",
               "sum_absgold_w", "n_quad1", "n_quad2", "n_quad3", "n_quad4")
GEO_METRICS = ("n_events", "sum_mentions", "sum_sources", "sum_w", "sum_gold_w",
               "sum_tone_w", "sum_gold", "sum_tone", "n_quad1", "n_quad2", "n_quad3", "n_quad4")
GKG_METRICS = ("total_docs", "security_docs", "political_docs", "economic_docs",
               "infrastructure_docs", "social_docs", "health_docs", "china_business_docs",
               "sum_tone", "sum_polarity")
SPECS = {
    "agg_relation": (REL_METRICS, ("actor1", "actor2", "root_code")),
    "agg_geo": (GEO_METRICS, ("geo_country", "root_code")),
    "agg_gkg": (GKG_METRICS, ("country",)),
}


def utcnow():
    return datetime.now(timezone.utc)


def bucket_of(timestamp: int, granularity: str):
    size = HOUR if granularity == "hour" else DAY
    return timestamp - timestamp % size


def mention_weight(n):
    return 1 + min(max(n, 0), MENTION_CAP) / 10


def goldstein_unit(v):
    return max(-1., min(1., (v or 0) / 10))


def tone_unit(v):
    return goldstein_unit(v)


class Store:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self, write=False):
        if hasattr(self, '_read_connection'):
            if write:
                raise RuntimeError('不能向查询快照写入数据')
            yield self._read_connection
            return
        db = sqlite3.connect(self.path, timeout=60)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=60000")
        db.execute("PRAGMA foreign_keys=ON")
        if write:
            db.execute("BEGIN IMMEDIATE")
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    @contextmanager
    def read_snapshot(self):
        with self.connect() as db:
            db.execute('BEGIN')
            view = copy(self)
            view._read_connection = db
            yield view

    def initialize(self):
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
            db.execute("PRAGMA auto_vacuum=INCREMENTAL")
            for table, (metrics, dims) in SPECS.items():
                columns = ",".join(f"{m} REAL NOT NULL DEFAULT 0" for m in metrics)
                keys = ("granularity", "bucket", *dims)
                db.execute(f"CREATE TABLE IF NOT EXISTS {table} (granularity TEXT NOT NULL, "
                           f"bucket INTEGER NOT NULL, " + ",".join(f"{d} TEXT NOT NULL" for d in dims)
                           + f",{columns},PRIMARY KEY ({','.join(keys)})) WITHOUT ROWID")
                db.execute(f"CREATE INDEX IF NOT EXISTS ix_{table}_country ON {table} "
                           f"(granularity,{dims[0]},bucket)")
                if table == "agg_relation":
                    db.execute("CREATE INDEX IF NOT EXISTS ix_relation_a2 ON agg_relation(granularity,actor2,bucket)")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS gdelt_state (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE IF NOT EXISTS ingest_file (
                    kind TEXT NOT NULL, file_ts INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
                    next_retry INTEGER NOT NULL DEFAULT 0, row_count INTEGER NOT NULL DEFAULT 0,
                    skipped_rows INTEGER NOT NULL DEFAULT 0, error TEXT, done_at INTEGER,
                    PRIMARY KEY(kind,file_ts)) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS ix_file_pending ON ingest_file(status,next_retry,file_ts);
                CREATE INDEX IF NOT EXISTS ix_file_time ON ingest_file(kind,file_ts,status);
                CREATE INDEX IF NOT EXISTS ix_file_work ON ingest_file(status,file_ts);
            """)

    def get_state(self, key, default=None):
        with self.connect() as db:
            row = db.execute("SELECT value FROM gdelt_state WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    @staticmethod
    def _state(db, key, value):
        db.execute("INSERT INTO gdelt_state VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                   (key, json.dumps(value, ensure_ascii=False, allow_nan=False)))

    def set_state(self, key, value):
        with self.connect(write=True) as db:
            self._state(db, key, value)

    def enqueue(self, start: int, end: int, cursor=None):
        """Schedule [start,end) in 15-minute slots; cursor update is atomic."""
        start -= start % SLOT
        end -= end % SLOT
        with self.connect(write=True) as db:
            before = db.total_changes
            db.executemany("INSERT INTO ingest_file(kind,file_ts) VALUES (?,?) "
                           "ON CONFLICT(kind,file_ts) DO UPDATE SET status='pending',attempts=0,next_retry=0,error=NULL "
                           "WHERE ingest_file.status='expired'",
                           ((kind, ts) for ts in range(start, end, SLOT) for kind in ("events", "gkg")))
            count = db.total_changes - before
            if cursor is not None:
                self._state(db, "scheduled_until", cursor)
        return count

    def pending(self, limit, now=None):
        now = now if now is not None else int(utcnow().timestamp())
        if limit <= 0:
            return []
        with self.connect() as db:
            # Reserve part of each batch for retries and recent files, so a multi-year
            # history queue cannot starve live updates or indefinitely defer failures.
            quota = max(1, limit//4)
            rows = list(db.execute("SELECT * FROM ingest_file WHERE status='failed' AND next_retry<=? "
                                   "ORDER BY next_retry,file_ts,kind LIMIT ?", (now, quota)))
            rows += list(db.execute("SELECT * FROM ingest_file WHERE status='pending' AND file_ts>=? "
                                    "ORDER BY file_ts,kind LIMIT ?", (now-DAY, min(quota, limit-len(rows)))))
            excluded = ' AND (kind,file_ts) NOT IN ('+','.join('(?,?)' for _ in rows)+')' if rows else ''
            args = [v for r in rows for v in (r['kind'], r['file_ts'])]
            rows += list(db.execute(
                "SELECT * FROM ingest_file WHERE status IN ('pending','failed') AND next_retry<=? "
                + excluded + " ORDER BY attempts,file_ts,kind LIMIT ?", (now, *args, limit-len(rows))))
            return [dict(r) for r in rows]

    def mark_failed(self, kind, ts, error):
        now = int(utcnow().timestamp())
        with self.connect(write=True) as db:
            db.execute("INSERT OR IGNORE INTO ingest_file(kind,file_ts) VALUES (?,?)", (kind, ts))
            row = db.execute("SELECT attempts,status FROM ingest_file WHERE kind=? AND file_ts=?", (kind, ts)).fetchone()
            if row["status"] == "done":
                return
            attempt = row["attempts"] + 1
            delay = min(21600, 60 * 2 ** min(attempt, 9))
            db.execute("UPDATE ingest_file SET status='failed',attempts=?,next_retry=?,error=? WHERE kind=? AND file_ts=?",
                       (attempt, now + delay, str(error)[:1000], kind, ts))

    def has_unfinished(self):
        with self.connect() as db:
            return db.execute("SELECT 1 FROM ingest_file WHERE status IN ('pending','failed') LIMIT 1").fetchone() is not None

    def retry_delay(self):
        with self.connect() as db:
            ts = db.execute("SELECT MIN(next_retry) FROM ingest_file WHERE status IN ('pending','failed')").fetchone()[0]
        return max(2, ts-int(utcnow().timestamp())) if ts is not None else 900

    def retry_failed(self):
        with self.connect(write=True) as db:
            return db.execute("UPDATE ingest_file SET next_retry=0 WHERE status='failed'").rowcount

    def apply(self, kind, ts, tables, rows, skipped=0):
        """Commit file ledger and direct daily/hourly contributions together."""
        with self.connect(write=True) as db:
            status = db.execute("SELECT status FROM ingest_file WHERE kind=? AND file_ts=?", (kind, ts)).fetchone()
            if status and status[0] == "done":
                return False
            for table, buckets in tables.items():
                metrics, dims = SPECS[table]
                keys = ("granularity", "bucket", *dims)
                combined = {}
                for key, values in buckets.items():
                    for gran in ("hour", "day"):
                        target = (gran, bucket_of(key[1], gran), *key[2:])
                        cell = combined.setdefault(target, {m: 0 for m in metrics})
                        for m in metrics:
                            value = values.get(m, 0)
                            if not math.isfinite(value):
                                raise ValueError("聚合数据包含非有限数字")
                            cell[m] += value
                cols = (*keys, *metrics)
                updates = ",".join(f"{m}={m}+excluded.{m}" for m in metrics)
                db.executemany(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)}) "
                               f"ON CONFLICT({','.join(keys)}) DO UPDATE SET {updates}",
                               (tuple(key) + tuple(v[m] for m in metrics) for key, v in combined.items()))
            db.execute("INSERT INTO ingest_file(kind,file_ts,status,row_count,skipped_rows,done_at) VALUES (?,?,'done',?,?,?) "
                       "ON CONFLICT(kind,file_ts) DO UPDATE SET status='done',row_count=excluded.row_count,"
                       "skipped_rows=excluded.skipped_rows,done_at=excluded.done_at,error=NULL,next_retry=0",
                       (kind, ts, rows, skipped, int(utcnow().timestamp())))
            version = db.execute("SELECT value FROM gdelt_state WHERE key='data_version'").fetchone()
            self._state(db, "data_version", (json.loads(version[0]) if version else 0) + 1)
        return True

    def coverage_buckets(self, kind, start, end, step):
        """Only fully elapsed slots count; missing collection is distinct from zero."""
        horizon = int(utcnow().timestamp()) // SLOT * SLOT
        with self.connect() as db:
            rows = db.execute("SELECT file_ts-file_ts%? bucket,COUNT(*) n FROM ingest_file "
                              "WHERE kind=? AND status='done' AND file_ts>=? AND file_ts<? GROUP BY bucket",
                              (step, kind, start, min(end, horizon))).fetchall()
        done = {r["bucket"]: r["n"] for r in rows}
        out = {}
        for b in range(start // step * step, end, step):
            expected = max(0, (min(b+step, horizon, end)-max(b, start)) // SLOT)
            count = done.get(b, 0)
            out[b] = {"expected": expected, "done": count,
                      "complete": expected > 0 and count == expected and b+step <= horizon}
        return out

    def coverage(self, kind, start, end):
        buckets = self.coverage_buckets(kind, start, end, DAY)
        expected = sum(v["expected"] for v in buckets.values())
        done = sum(v["done"] for v in buckets.values())
        with self.connect() as db:
            latest = db.execute("SELECT MAX(file_ts) FROM ingest_file WHERE kind=? AND status='done'", (kind,)).fetchone()[0]
        return {"source": kind, "expected_files": expected, "completed_files": done,
                "coverage_percent": round(done/expected*100, 2) if expected else 0,
                "complete_days": sum(v["complete"] for v in buckets.values()),
                "last_file_at": datetime.fromtimestamp(latest, timezone.utc).isoformat() if latest is not None else None,
                "last_file_ts": latest, "stale": latest is None or int(utcnow().timestamp())-latest > 3*SLOT,
                "timezone": "UTC"}

    def prune(self, hour_days=60, day_days=730):
        now = bucket_of(int(utcnow().timestamp()), "day")
        deleted = {}
        with self.connect(write=True) as db:
            for table in SPECS:
                for gran, days in (("hour", hour_days), ("day", day_days)):
                    cursor = db.execute(f"DELETE FROM {table} WHERE granularity=? AND bucket<?", (gran, now-days*DAY))
                    deleted[f"{table}.{gran}"] = cursor.rowcount
            # Keep the compact ledger: pruning it would permit reimport double counting.
            db.execute("UPDATE ingest_file SET status='expired',error='批次超出聚合保留期' "
                       "WHERE status IN ('pending','failed') AND file_ts<?", (now-day_days*DAY,))
        with self.connect() as db:
            db.execute("PRAGMA wal_checkpoint(PASSIVE)")
            db.execute("PRAGMA incremental_vacuum(2000)")
        return deleted

    def stats(self):
        with self.connect() as db:
            ledger = {r["status"]: r["n"] for r in db.execute("SELECT status,COUNT(*) n FROM ingest_file GROUP BY status")}
            latest = {r["kind"]: r["ts"] for r in db.execute("SELECT kind,MAX(file_ts) ts FROM ingest_file WHERE status='done' GROUP BY kind")}
            errors = [dict(r) for r in db.execute("SELECT kind,file_ts,attempts,error,next_retry FROM ingest_file WHERE status='failed' ORDER BY file_ts DESC LIMIT 8")]
            pages = db.execute("PRAGMA page_count").fetchone()[0]
            free = db.execute("PRAGMA freelist_count").fetchone()[0]
            counts = db.execute("SELECT COALESCE(SUM(row_count),0) parsed,COALESCE(SUM(skipped_rows),0) skipped "
                                "FROM ingest_file WHERE status='done'").fetchone()
        files = [self.path, Path(str(self.path)+"-wal"), Path(str(self.path)+"-shm")]
        usage = shutil.disk_usage(self.path.parent)
        return {"ledger": ledger, "latest": latest, "errors": errors,
                "database_bytes": sum(p.stat().st_size for p in files if p.exists()),
                "free_disk_bytes": usage.free, "free_page_ratio": free/pages if pages else 0,
                "raw_data_retained": False, "parsed_rows": counts["parsed"], "skipped_rows": counts["skipped"]}
