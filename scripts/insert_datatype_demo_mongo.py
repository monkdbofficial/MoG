#!/usr/bin/env python3
from __future__ import annotations

import base64
import datetime as dt
import logging
import os
import random
import shutil
import socket
import sys
import time
import traceback
import uuid
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit


def _require_pymongo() -> tuple[Any, Any, Any, Any, Any, Any, Any]:
    try:
        import pymongo  # type: ignore
        from pymongo import ASCENDING, DESCENDING, InsertOne, MongoClient, ReplaceOne  # type: ignore
        from pymongo.errors import BulkWriteError  # type: ignore
        return (pymongo, ASCENDING, DESCENDING, MongoClient, ReplaceOne, BulkWriteError, InsertOne)
    except Exception as e:
        raise SystemExit("Missing dependency: pymongo. Install with: pip install pymongo") from e


def _require_bson_types() -> dict[str, Any]:
    try:
        from bson import (  # type: ignore
            Binary,
            Code,
            DBRef,
            Decimal128,
            Int64,
            MaxKey,
            MinKey,
            ObjectId,
            Regex,
            Timestamp,
        )
        return {
            "Binary": Binary,
            "Code": Code,
            "DBRef": DBRef,
            "Decimal128": Decimal128,
            "Int64": Int64,
            "MaxKey": MaxKey,
            "MinKey": MinKey,
            "ObjectId": ObjectId,
            "Regex": Regex,
            "Timestamp": Timestamp,
        }
    except Exception as e:
        raise SystemExit("Missing bson types (from pymongo). Install with: pip install pymongo") from e


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _redact_uri(uri: str) -> str:
    try:
        parts = urlsplit(uri)
        if not parts.username and not parts.password:
            return uri
        netloc = parts.hostname or ""
        if parts.port:
            netloc = f"{netloc}:{parts.port}"
        if parts.username:
            netloc = f"{parts.username}:***@{netloc}"
        return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    except Exception:
        return "<redacted>"


def _safe_repr(value: Any, limit: int = 500) -> str:
    try:
        s = repr(value)
        return s if len(s) <= limit else s[:limit] + "...<truncated>"
    except Exception:
        return "<unrepr-able>"


def _get_memory_hint() -> str:
    try:
        import resource  # unix only

        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            rss_mb = rss_kb / (1024 * 1024)
        else:
            rss_mb = rss_kb / 1024
        return f"{rss_mb:.1f} MB maxrss"
    except Exception:
        return "n/a"


def setup_logging(log_file: str = "seed_sample.debug.log", verbose_pymongo: bool = False) -> logging.Logger:
    logger = logging.getLogger("mog_seed")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    if verbose_pymongo:
        logging.getLogger("pymongo").setLevel(logging.DEBUG)
        logging.getLogger("pymongo.command").setLevel(logging.DEBUG)
        logging.getLogger("pymongo.connection").setLevel(logging.DEBUG)
        logging.getLogger("pymongo.serverSelection").setLevel(logging.DEBUG)

        for name in ("pymongo", "pymongo.command", "pymongo.connection", "pymongo.serverSelection"):
            plog = logging.getLogger(name)
            plog.handlers.clear()
            plog.addHandler(file_handler)
            plog.propagate = False

    logger.debug("logging initialized file=%s verbose_pymongo=%s pid=%s", log_file, verbose_pymongo, os.getpid())
    return logger


class Console:
    def __init__(self) -> None:
        self.is_tty = sys.stdout.isatty()

    def _c(self, code: str) -> str:
        if not self.is_tty:
            return ""
        return code

    @property
    def reset(self) -> str:
        return self._c("\033[0m")

    @property
    def dim(self) -> str:
        return self._c("\033[2m")

    @property
    def bold(self) -> str:
        return self._c("\033[1m")

    @property
    def green(self) -> str:
        return self._c("\033[32m")

    @property
    def yellow(self) -> str:
        return self._c("\033[33m")

    @property
    def red(self) -> str:
        return self._c("\033[31m")

    @property
    def cyan(self) -> str:
        return self._c("\033[36m")

    def hr(self, ch: str = "─") -> None:
        width = shutil.get_terminal_size((100, 20)).columns
        print(self.dim + (ch * max(40, min(width, 120))) + self.reset)

    def h1(self, title: str) -> None:
        self.hr("═")
        print(f"{self.bold}{title}{self.reset}")
        self.hr("═")

    def section(self, title: str) -> None:
        print(f"\n{self.bold}{self.cyan}{title}{self.reset}")
        self.hr()

    def kv(self, items: dict[str, Any]) -> None:
        for k, v in items.items():
            print(f"{self.dim}{k:>22}{self.reset} : {v}")

    def ok(self, msg: str) -> None:
        print(f"{self.green}✔{self.reset} {msg}")

    def warn(self, msg: str) -> None:
        print(f"{self.yellow}▲{self.reset} {msg}")

    def info(self, msg: str) -> None:
        print(f"{self.cyan}•{self.reset} {msg}")

    def fail(self, msg: str) -> None:
        print(f"{self.red}✖{self.reset} {msg}")


def _rand_str(n: int) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(random.choice(alphabet) for _ in range(n))


def _rand_bytes(n: int) -> bytes:
    return os.urandom(n)


def _rand_geo_point() -> dict[str, Any]:
    lon = random.uniform(-179.99, 179.99)
    lat = random.uniform(-89.99, 89.99)
    return {"type": "Point", "coordinates": [lon, lat]}


def _doc_debug_summary(doc: dict[str, Any]) -> dict[str, Any]:
    telemetry_count = len(doc.get("telemetry", []))
    event_count = len(doc.get("events", []))
    map_of_maps = doc.get("map_of_maps", {})
    weird_keys = list((doc.get("weird_keys") or {}).keys())
    raw_binary = (((doc.get("raw") or {}).get("binary")))
    binary_len = len(raw_binary) if raw_binary is not None else 0
    return {
        "_id": str(doc.get("_id")),
        "mission": doc.get("mission"),
        "seq": doc.get("seq"),
        "telemetry_count": telemetry_count,
        "event_count": event_count,
        "map_of_maps_keys": len(map_of_maps),
        "weird_keys": weird_keys,
        "binary_len": binary_len,
        "unicode_sample": ((doc.get("strings") or {}).get("unicode")),
    }


def _make_big_nested_document(*, mission: str, seq: int) -> dict[str, Any]:
    bson = _require_bson_types()
    ObjectId = bson["ObjectId"]
    Binary = bson["Binary"]
    Decimal128 = bson["Decimal128"]
    Int64 = bson["Int64"]
    Timestamp = bson["Timestamp"]
    MinKey = bson["MinKey"]
    MaxKey = bson["MaxKey"]
    Regex = bson["Regex"]
    Code = bson["Code"]
    DBRef = bson["DBRef"]

    now = _utc_now()
    start = now - dt.timedelta(days=random.randint(1, 3650))
    end = start + dt.timedelta(seconds=random.randint(1, 10_000_000))

    image_bytes = _rand_bytes(random.randint(64, 256))
    image_b64 = base64.b64encode(image_bytes).decode("ascii")

    payload = {
        "_id": ObjectId(),
        "mission": mission,
        "seq": seq,
        "ingest_ts": now,
        "device": {
            "satellite_id": f"{mission}-{random.randint(1, 99):02d}",
            "boot_id": uuid.uuid4(),
            "firmware": {"major": 3, "minor": random.randint(0, 99), "patch": random.randint(0, 99)},
            "calibration": {
                "bias": Decimal128(str(round(random.uniform(-0.05, 0.05), 6))),
                "scale": Decimal128(str(round(random.uniform(0.95, 1.05), 6))),
                "matrix": [[round(random.random(), 6) for _ in range(3)] for _ in range(3)],
            },
        },
        "orbit": {
            "t0": start,
            "t1": end,
            "ecef_km": {
                "x": Decimal128(str(round(random.uniform(-42164, 42164), 6))),
                "y": Decimal128(str(round(random.uniform(-42164, 42164), 6))),
                "z": Decimal128(str(round(random.uniform(-42164, 42164), 6))),
            },
            "ground_track": {
                "type": "LineString",
                "coordinates": [
                    [random.uniform(-179.99, 179.99), random.uniform(-89.99, 89.99)]
                    for _ in range(20)
                ],
            },
        },
        "telemetry": [
            {
                "ts": now - dt.timedelta(seconds=i * random.randint(1, 5)),
                "temp_c": round(random.uniform(-120, 120), 3),
                "voltage_v": Decimal128(str(round(random.uniform(3.0, 14.0), 6))),
                "current_a": Decimal128(str(round(random.uniform(0.0, 5.0), 6))),
                "status": random.choice(["OK", "WARN", "ALARM"]),
            }
            for i in range(200)
        ],
        "events": [
            {
                "event_id": ObjectId(),
                "kind": random.choice(["maneuver", "downlink", "uplink", "safe_mode", "radiation"]),
                "at": now - dt.timedelta(minutes=random.randint(0, 60 * 24 * 30)),
                "tags": [random.choice(["science", "ops", "nav", "payload", "thermal"]) for _ in range(3)],
            }
            for _ in range(10)
        ],
        "geo": {
            "point": _rand_geo_point(),
            "footprint": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [random.uniform(-179.99, 179.99), random.uniform(-89.99, 89.99)]
                        for _ in range(10)
                    ]
                ],
            },
        },
        "raw": {
            "binary": Binary(image_bytes),
            "binary_b64_shadow": image_b64,
            "int64": Int64(random.randint(0, 2**63 - 1)),
            "timestamp": Timestamp(random.randint(1, 2_000_000_000), random.randint(0, 9999)),
            "minkey": MinKey(),
            "maxkey": MaxKey(),
            "regex": Regex(r"^MOG_[A-Z0-9]{8}$", "i"),
            "code": Code("function(x){ return x + 1; }", scope={"seed": seq}),
            "dbref": DBRef(collection="missions", id=ObjectId()),
        },
        "strings": {
            "ascii": _rand_str(32),
            "unicode": "MonkDB/मोंकडीबी/モンクDB/数据库/🚀",
        },
        "arrays": {
            "ints": [random.randint(-1000, 1000) for _ in range(256)],
            "mixed": [None, True, 3.14159, "str", {"nested": {"x": 1}}, [1, 2, 3]],
        },
        "map_of_maps": {f"k{i:04d}": {f"sub{j:03d}": _rand_str(12) for j in range(8)} for i in range(50)},
        "flags": {
            "retained": random.choice([True, False]),
            "priority": random.choice(["P0", "P1", "P2", "P3"]),
        },
    }

    payload["weird_keys"] = {
        "spaces allowed": "yes",
        "dotted.key": "mongodb supports it but many tools don’t like it",
        "dollar$sign": "ok-ish as a field name in BSON, but often problematic downstream",
    }

    return payload


def _ensure_indexes(source_db, console: Console | None = None, logger: logging.Logger | None = None) -> None:
    _, ASCENDING, _, _, _, _, _ = _require_pymongo()

    def _idx(coll: str, spec: list[tuple[str, Any]], *, name: str, **kwargs: Any) -> None:
        try:
            t0 = time.time()
            if logger:
                logger.debug("create_index start coll=%s name=%s spec=%s kwargs=%s", coll, name, spec, kwargs)
            source_db[coll].create_index(spec, name=name, **kwargs)
            took = time.time() - t0
            if console:
                console.ok(f"index {coll}.{name} created")
            if logger:
                logger.info("create_index done coll=%s name=%s took=%.3fs", coll, name, took)
        except Exception as e:
            if console:
                console.warn(f"index {coll}.{name} failed: {e}")
            if logger:
                logger.exception("create_index failed coll=%s name=%s spec=%s kwargs=%s", coll, name, spec, kwargs)

    _idx("telemetry", [("mission", ASCENDING), ("seq", -1)], name="mission_seq")
    _idx("telemetry", [("ingest_ts", -1)], name="ingest_ts_desc")
    _idx("telemetry", [("geo.point", "2dsphere")], name="geo_point_2dsphere")
    _idx("telemetry", [("strings.unicode", "text")], name="unicode_text")
    _idx("events", [("mission", ASCENDING), ("events.kind", ASCENDING)], name="mission_event_kind")
    _idx("events", [("events.at", -1)], name="event_at_desc")
    _idx("missions", [("mission", ASCENDING)], name="mission_unique", unique=True)
    _idx("logs", [("expires_at", ASCENDING)], name="ttl_expires", expireAfterSeconds=0)


def _flush_bulk(
    db,
    coll: str,
    ops: list[Any],
    *,
    console: Console | None = None,
    logger: logging.Logger | None = None,
    phase: str = "regular",
) -> dict[str, Any]:
    _, _, _, _, _, BulkWriteError, _ = _require_pymongo()

    if not ops:
        if logger:
            logger.debug("bulk skip coll=%s phase=%s reason=no_ops", coll, phase)
        return {"ops": 0, "upserts": 0, "modified": 0, "matched": 0, "took": 0.0, "errors": 0}

    try:
        t0 = time.time()
        if logger:
            logger.debug("bulk_write start coll=%s phase=%s ops=%s mem=%s", coll, phase, len(ops), _get_memory_hint())
        res = db[coll].bulk_write(ops, ordered=False)
        took = time.time() - t0
        out = {
            "ops": len(ops),
            "upserts": getattr(res, "upserted_count", 0),
            "modified": getattr(res, "modified_count", 0),
            "matched": getattr(res, "matched_count", 0),
            "took": took,
            "errors": 0,
        }
        if console:
            console.info(
                f"flushed {coll} ({len(ops)} ops, upserts={out['upserts']}, matched={out['matched']}, modified={out['modified']}, took={took:.3f}s)"
            )
        if logger:
            logger.info(
                "bulk_write done coll=%s phase=%s ops=%s upserts=%s matched=%s modified=%s took=%.3fs",
                coll, phase, out["ops"], out["upserts"], out["matched"], out["modified"], out["took"]
            )
        return out
    except BulkWriteError as bwe:
        details = getattr(bwe, "details", {}) or {}
        write_errors = details.get("writeErrors") or []
        first = write_errors[0] if write_errors else {}
        msg = first.get("errmsg") or first.get("message") or _safe_repr(first)
        code = first.get("code")
        if console:
            console.warn(f"bulk_write errors for {coll} count={len(write_errors)} code={code} msg={msg[:160]}")
        if logger:
            logger.exception(
                "bulk_write error coll=%s phase=%s ops=%s errors=%s first_code=%s first_msg=%s",
                coll, phase, len(ops), len(write_errors), code, msg[:500]
            )
        return {"ops": len(ops), "upserts": 0, "modified": 0, "matched": 0, "took": 0.0, "errors": len(write_errors)}
    except Exception:
        if logger:
            logger.exception("bulk_write unexpected failure coll=%s phase=%s ops=%s", coll, phase, len(ops))
        raise


def seed_source(
    *,
    source_uri: str,
    db_name: str,
    docs: int,
    missions: int,
    drop_db: bool,
    batch: int,
    use_upsert: bool,
    progress_every_docs: int = 0,
    console: Console | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    bson = _require_bson_types()
    ObjectId = bson["ObjectId"]
    Binary = bson["Binary"]
    _, _, _, MongoClient, ReplaceOne, _, InsertOne = _require_pymongo()

    if logger:
        logger.info(
            "seed_source start db=%s docs=%s missions=%s batch=%s drop_db=%s uri=%s",
            db_name, docs, missions, batch, drop_db, _redact_uri(source_uri)
        )

    client = MongoClient(
        source_uri,
        appname="mog-migrator-seed",
        serverSelectionTimeoutMS=15000,
        uuidRepresentation="standard",
    )

    stats = {
        "generated_docs": 0,
        "missions_upserts": 0,
        "telemetry_flushes": 0,
        "events_flushes": 0,
        "logs_flushes": 0,
        "images_flushes": 0,
        "telemetry_ops": 0,
        "events_ops": 0,
        "logs_ops": 0,
        "images_ops": 0,
        "bulk_errors": 0,
    }

    try:
        t0 = time.time()
        client.admin.command("ping")
        if console:
            console.ok("connected to MongoDB")
        if logger:
            logger.info("mongodb ping ok took=%.3fs", time.time() - t0)

        db = client[db_name]

        if drop_db:
            t0 = time.time()
            if logger:
                logger.warning("dropping database db=%s", db_name)
            client.drop_database(db_name)
            if console:
                console.warn(f"dropped database {db_name}")
            if logger:
                logger.warning("drop database completed db=%s took=%.3fs", db_name, time.time() - t0)

        mission_names = [f"NOVA-{i:02d}" for i in range(1, missions + 1)]
        if logger:
            logger.debug("mission_names=%s", mission_names)

        try:
            t0 = time.time()
            res = db["missions"].bulk_write(
                [
                    ReplaceOne(
                        {"mission": m},
                        {"mission": m, "created_at": _utc_now(), "owner": random.choice(["ops", "science", "nav"])},
                        upsert=True,
                    )
                    for m in mission_names
                ],
                ordered=False,
            )
            stats["missions_upserts"] = getattr(res, "upserted_count", 0)
            if console:
                console.ok(f"missions upserted ({res.upserted_count} upserts, {len(mission_names)} ops)")
            if logger:
                logger.info(
                    "missions bulk_write done ops=%s upserts=%s matched=%s modified=%s took=%.3fs",
                    len(mission_names),
                    getattr(res, "upserted_count", 0),
                    getattr(res, "matched_count", 0),
                    getattr(res, "modified_count", 0),
                    time.time() - t0,
                )
        except Exception:
            if console:
                console.warn("missions upsert failed")
            if logger:
                logger.exception("missions upsert failed")

        if console:
            console.info("creating indexes (best-effort)")
        _ensure_indexes(db, console=console, logger=logger)

        telemetry_ops: list[Any] = []
        events_ops: list[Any] = []
        logs_ops: list[Any] = []
        image_ops: list[Any] = []

        started = time.time()
        last_report = started
        last_report_docs = 0
        batch_no = 0

        for i in range(docs):
            loop_started = time.time()
            mission = random.choice(mission_names)

            if logger and (i < 3 or (i + 1) % 100 == 0):
                logger.debug(
                    "doc_loop start doc=%s/%s mission=%s pending telemetry=%s events=%s logs=%s images=%s mem=%s",
                    i + 1, docs, mission, len(telemetry_ops), len(events_ops), len(logs_ops), len(image_ops), _get_memory_hint()
                )

            t_doc = time.time()
            doc = _make_big_nested_document(mission=mission, seq=i)
            build_took = time.time() - t_doc
            stats["generated_docs"] += 1

            if logger and (i < 3 or (i + 1) % 250 == 0):
                logger.debug(
                    "generated doc summary=%s build_took=%.3fs",
                    _safe_repr(_doc_debug_summary(doc)),
                    build_took,
                )

            if use_upsert:
                telemetry_ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
            else:
                telemetry_ops.append(InsertOne(doc))
            stats["telemetry_ops"] += 1

            event_id = ObjectId()
            evt_doc = {"_id": event_id, "mission": mission, "ts": _utc_now(), "events": doc["events"]}
            if use_upsert:
                events_ops.append(ReplaceOne({"_id": event_id}, evt_doc, upsert=True))
            else:
                events_ops.append(InsertOne(evt_doc))
            stats["events_ops"] += 1

            log_id = ObjectId()
            log_doc = {
                "_id": log_id,
                "mission": mission,
                "level": random.choice(["debug", "info", "warn", "error"]),
                "msg": f"simulated log {i} {_rand_str(16)}",
                "attrs": {
                    "host": _rand_str(8),
                    "trace": uuid.uuid4(),
                    "build_took_ms": round(build_took * 1000, 2),
                    "doc_seq": i,
                },
                "ts": _utc_now(),
                "expires_at": _utc_now() + dt.timedelta(days=3650),
            }
            if use_upsert:
                logs_ops.append(ReplaceOne({"_id": log_id}, log_doc, upsert=True))
            else:
                logs_ops.append(InsertOne(log_doc))
            stats["logs_ops"] += 1

            image_id = ObjectId()
            img_doc = {
                "_id": image_id,
                "mission": mission,
                "ts": _utc_now(),
                "content_type": "application/octet-stream",
                "blob": Binary(_rand_bytes(1024)),
                "sha256": _rand_str(64),
            }
            if use_upsert:
                image_ops.append(ReplaceOne({"_id": image_id}, img_doc, upsert=True))
            else:
                image_ops.append(InsertOne(img_doc))
            stats["images_ops"] += 1

            if logger and (i < 3 or (i + 1) % 250 == 0):
                logger.debug(
                    "queues updated doc=%s sizes telemetry=%s events=%s logs=%s images=%s loop_took=%.3fs",
                    i + 1, len(telemetry_ops), len(events_ops), len(logs_ops), len(image_ops), time.time() - loop_started
                )

            if (i + 1) % batch == 0:
                batch_no += 1
                if logger:
                    logger.info(
                        "batch flush start batch_no=%s ending_doc=%s pending telemetry=%s events=%s logs=%s images=%s",
                        batch_no, i + 1, len(telemetry_ops), len(events_ops), len(logs_ops), len(image_ops)
                    )

                if logger:
                    logger.info("about to flush telemetry batch_no=%s ops=%s", batch_no, len(telemetry_ops))
                out = _flush_bulk(db, "telemetry", telemetry_ops, console=console, logger=logger, phase=f"batch-{batch_no}")
                if logger:
                    logger.info("finished flush telemetry batch_no=%s result=%s", batch_no, out)

                if logger:
                    logger.info("about to flush events batch_no=%s ops=%s", batch_no, len(events_ops))
                out = _flush_bulk(db, "events", events_ops, console=console, logger=logger, phase=f"batch-{batch_no}")
                if logger:
                    logger.info("finished flush events batch_no=%s result=%s", batch_no, out)

                if logger:
                    logger.info("about to flush logs batch_no=%s ops=%s", batch_no, len(logs_ops))
                out = _flush_bulk(db, "logs", logs_ops, console=console, logger=logger, phase=f"batch-{batch_no}")
                if logger:
                    logger.info("finished flush logs batch_no=%s result=%s", batch_no, out)

                if logger:
                    logger.info("about to flush images batch_no=%s ops=%s", batch_no, len(image_ops))
                out = _flush_bulk(db, "images", image_ops, console=console, logger=logger, phase=f"batch-{batch_no}")
                if logger:
                    logger.info("finished flush images batch_no=%s result=%s", batch_no, out)

                telemetry_ops.clear()
                events_ops.clear()
                logs_ops.clear()
                image_ops.clear()

                if logger:
                    logger.info(
                        "batch flush end batch_no=%s queues_cleared telemetry=%s events=%s logs=%s images=%s mem=%s",
                        batch_no, len(telemetry_ops), len(events_ops), len(logs_ops), len(image_ops), _get_memory_hint()
                    )

            if progress_every_docs and (i + 1) % progress_every_docs == 0:
                now = time.time()
                delta = now - last_report
                done = i + 1
                docs_delta = done - last_report_docs
                rate = (docs_delta / delta) if delta > 0 else 0.0
                elapsed = now - started
                eta = ((docs - done) / rate) if rate > 0 else -1
                if console:
                    console.info(
                        f"progress {done}/{docs} docs ({rate:,.1f} docs/s, elapsed {elapsed:,.1f}s, eta {eta:,.1f}s)"
                    )
                if logger:
                    logger.info(
                        "progress done=%s total=%s rate=%.1f_docs_per_sec elapsed=%.1fs eta=%.1fs mem=%s stats=%s",
                        done, docs, rate, elapsed, eta, _get_memory_hint(), _safe_repr(stats, 800)
                    )
                last_report = now
                last_report_docs = done

        if logger:
            logger.info(
                "final flush start remaining telemetry=%s events=%s logs=%s images=%s",
                len(telemetry_ops), len(events_ops), len(logs_ops), len(image_ops)
            )

        if telemetry_ops:
            out = _flush_bulk(db, "telemetry", telemetry_ops, console=console, logger=logger, phase="final")
            stats["telemetry_flushes"] += 1
            stats["bulk_errors"] += out["errors"]
        if events_ops:
            out = _flush_bulk(db, "events", events_ops, console=console, logger=logger, phase="final")
            stats["events_flushes"] += 1
            stats["bulk_errors"] += out["errors"]
        if logs_ops:
            out = _flush_bulk(db, "logs", logs_ops, console=console, logger=logger, phase="final")
            stats["logs_flushes"] += 1
            stats["bulk_errors"] += out["errors"]
        if image_ops:
            out = _flush_bulk(db, "images", image_ops, console=console, logger=logger, phase="final")
            stats["images_flushes"] += 1
            stats["bulk_errors"] += out["errors"]

        seconds = time.time() - started

        collection_counts = {}
        for coll in ["missions", "telemetry", "events", "logs", "images"]:
            try:
                t0 = time.time()
                count = db[coll].estimated_document_count()
                collection_counts[coll] = count
                if logger:
                    logger.debug("estimated_document_count coll=%s count=%s took=%.3fs", coll, count, time.time() - t0)
            except Exception:
                collection_counts[coll] = "n/a"
                if logger:
                    logger.exception("estimated_document_count failed coll=%s", coll)

        if logger:
            logger.info(
                "seed_source complete db=%s docs=%s seconds=%.3f counts=%s final_stats=%s mem=%s",
                db_name, docs, seconds, collection_counts, _safe_repr(stats, 1200), _get_memory_hint()
            )

        return {
            "db": db_name,
            "docs_seeded": docs,
            "missions": missions,
            "seconds": round(seconds, 3),
            "collections": ["missions", "telemetry", "events", "logs", "images"],
            "collection_counts": collection_counts,
            "stats": stats,
        }
    finally:
        if logger:
            logger.info("closing mongodb client")
        client.close()


@dataclass(frozen=True)
class SeedConfig:
    source_uri: str = "mongodb://localhost:27017/admin"
    db_name: str = "mig_sample"
    docs: int = 250
    missions: int = 8
    drop_db: bool = False
    batch: int = 10
    use_upsert: bool = True
    progress_every_docs: int = 50
    show_server_info: bool = True
    random_seed: int | None = None
    log_file: str = "seed_sample.debug.log"
    verbose_pymongo: bool = False


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_config() -> SeedConfig:
    base = SeedConfig()
    return SeedConfig(
        source_uri=os.getenv("MOG_SEED_SOURCE_URI", base.source_uri),
        db_name=os.getenv("MOG_SEED_DB", base.db_name),
        docs=int(os.getenv("MOG_SEED_DOCS", str(base.docs))),
        missions=int(os.getenv("MOG_SEED_MISSIONS", str(base.missions))),
        drop_db=_env_bool("MOG_SEED_DROP_DB", base.drop_db),
        batch=int(os.getenv("MOG_SEED_BATCH", str(base.batch))),
        use_upsert=_env_bool("MOG_SEED_USE_UPSERT", base.use_upsert),
        progress_every_docs=int(os.getenv("MOG_SEED_PROGRESS_EVERY", str(base.progress_every_docs))),
        show_server_info=_env_bool("MOG_SEED_SHOW_SERVER_INFO", base.show_server_info),
        random_seed=(
            int(os.getenv("MOG_SEED_RANDOM_SEED", "0"))
            if os.getenv("MOG_SEED_RANDOM_SEED") is not None
            else base.random_seed
        ),
        log_file=os.getenv("MOG_SEED_LOG_FILE", base.log_file),
        verbose_pymongo=_env_bool("MOG_SEED_VERBOSE_PYMONGO", base.verbose_pymongo),
    )


def main() -> int:
    console = Console()
    cfg = load_config()
    logger = setup_logging(cfg.log_file, cfg.verbose_pymongo)

    if cfg.random_seed is not None:
        random.seed(cfg.random_seed)

    started = time.time()
    pymongo, _, _, MongoClient, _, _, _ = _require_pymongo()

    logger.info("starting seed job")
    logger.info("python=%s platform=%s host=%s pid=%s", sys.version.split()[0], sys.platform, socket.gethostname(), os.getpid())
    logger.info("pymongo=%s", getattr(pymongo, "__version__", "?"))
    logger.info("effective config=%s", _safe_repr(asdict(cfg), 1200))

    console.h1("MoG Seed Generator (MongoDB)")
    console.kv(
        {
            "host": socket.gethostname(),
            "python": sys.version.split()[0],
            "platform": sys.platform,
            "pymongo": getattr(pymongo, "__version__", "?"),
            "started": _utc_now().isoformat(),
            "pid": os.getpid(),
        }
    )

    console.section("Configuration")
    console.kv(
        {
            "source_uri": _redact_uri(cfg.source_uri),
            "db": cfg.db_name,
            "docs": cfg.docs,
            "missions": cfg.missions,
            "drop_db": cfg.drop_db,
            "batch": cfg.batch,
            "use_upsert": cfg.use_upsert,
            "progress_every": cfg.progress_every_docs,
            "random_seed": cfg.random_seed,
            "log_file": cfg.log_file,
            "verbose_pymongo": cfg.verbose_pymongo,
            "memory_hint": _get_memory_hint(),
        }
    )

    console.section("Connectivity")
    probe = MongoClient(
        cfg.source_uri,
        appname="mog-seed-probe",
        serverSelectionTimeoutMS=15000,
        uuidRepresentation="standard",
    )
    try:
        t0 = time.time()
        probe.admin.command("ping")
        console.ok("MongoDB ping ok")
        logger.info("probe ping ok took=%.3fs", time.time() - t0)

        if cfg.show_server_info:
            try:
                t0 = time.time()
                info = probe.server_info()
                console.kv(
                    {
                        "server_version": info.get("version"),
                        "git_version": info.get("gitVersion"),
                        "max_bson": info.get("maxBsonObjectSize"),
                        "max_msg": info.get("maxMessageSizeBytes"),
                    }
                )
                logger.info(
                    "server_info took=%.3fs version=%s git=%s max_bson=%s max_msg=%s",
                    time.time() - t0,
                    info.get("version"),
                    info.get("gitVersion"),
                    info.get("maxBsonObjectSize"),
                    info.get("maxMessageSizeBytes"),
                )
            except Exception:
                console.warn("server_info unavailable")
                logger.exception("server_info unavailable")
    finally:
        probe.close()

    console.section("Seeding")
    console.info("collections: missions, telemetry, events, logs, images")

    try:
        seed_out = seed_source(
            source_uri=cfg.source_uri,
            db_name=cfg.db_name,
            docs=cfg.docs,
            missions=cfg.missions,
            drop_db=cfg.drop_db,
            batch=cfg.batch,
            use_upsert=cfg.use_upsert,
            progress_every_docs=cfg.progress_every_docs,
            console=console,
            logger=logger,
        )
    except Exception as e:
        console.fail(f"seed failed: {e}")
        logger.exception("fatal seed failure traceback=%s", traceback.format_exc())
        return 1

    console.ok("seed completed")
    console.section("Summary")
    console.kv(seed_out | {"seconds_total": round(time.time() - started, 3), "finished": _utc_now().isoformat()})
    logger.info("seed completed total_seconds=%.3f summary=%s", time.time() - started, _safe_repr(seed_out, 1500))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


        #  seconds_total : 127.312
