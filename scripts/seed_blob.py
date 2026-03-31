#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any


def _require_pymongo() -> tuple[Any, Any]:
    try:
        from pymongo import MongoClient  # type: ignore
        from bson import Binary  # type: ignore

        return MongoClient, Binary
    except Exception as e:
        raise SystemExit("Missing dependency: pymongo. Install with: pip install pymongo") from e


def _sha1_hex(data: bytes) -> str:
    h = hashlib.sha1()
    h.update(data)
    return h.hexdigest()


def _read_payload(path: str | None, min_bytes: int) -> tuple[bytes, str]:
    if path:
        with open(path, "rb") as f:
            data = f.read()
        name = os.path.basename(path)
    else:
        data = os.urandom(max(min_bytes, 4096))
        name = "random.bin"

    if len(data) < min_bytes:
        pad = os.urandom(min_bytes - len(data))
        data = data + pad

    return data, name


def _http_get_bytes(url: str, timeout_s: float) -> bytes:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return resp.read()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Seed a document with BSON Binary so MoG offloads it into a MonkDB BLOB table.",
    )
    ap.add_argument(
        "--mongo-uri",
        default=os.environ.get("MOG_MONGO_URI", "mongodb://user:password@127.0.0.1:27017/admin"),
        help="MongoDB wire-protocol URI for MoG (default: env MOG_MONGO_URI or mongodb://user:password@127.0.0.1:27017/admin)",
    )
    ap.add_argument("--db", default="test", help="Database name (default: test)")
    ap.add_argument("--coll", default="blob_demo", help="Collection name (default: blob_demo)")
    ap.add_argument(
        "--field",
        default="payload",
        help="Field name to store BSON Binary into (default: payload)",
    )
    ap.add_argument(
        "--file",
        default=None,
        help="Optional path to a local file to upload as a blob (otherwise uses random bytes)",
    )
    ap.add_argument(
        "--min-bytes",
        type=int,
        default=512,
        help="Ensure payload is at least this many bytes (default: 512). Must be >= MOG_BLOB_MIN_BYTES on the server to trigger offload.",
    )
    ap.add_argument(
        "--blob-table",
        default=os.environ.get("MOG_BLOB_TABLE", "media"),
        help="Expected BLOB table name (default: env MOG_BLOB_TABLE or media). This must match MoG's MOG_BLOB_TABLE.",
    )
    ap.add_argument(
        "--blob-http-base",
        default=os.environ.get("MOG_BLOB_HTTP_BASE", "http://localhost:6000"),
        help="MonkDB HTTP base for /_blobs (default: env MOG_BLOB_HTTP_BASE or http://localhost:6000)",
    )
    ap.add_argument(
        "--verify-http",
        action="store_true",
        help="After insert, GET /_blobs/<table>/<sha1> and verify bytes match.",
    )
    ap.add_argument(
        "--timeout-s",
        type=float,
        default=10.0,
        help="HTTP timeout seconds for verify (default: 10)",
    )
    args = ap.parse_args()

    MongoClient, Binary = _require_pymongo()

    data, name = _read_payload(args.file, max(1, int(args.min_bytes)))
    sha1hex = _sha1_hex(data)

    print("MoG BLOB support must be enabled on the server process:")
    print(f"  export MOG_BLOB_TABLE={args.blob_table}")
    print(f"  export MOG_BLOB_HTTP_BASE={args.blob_http_base}")
    print("  export MOG_BLOB_MIN_BYTES=256   # default")
    print("")

    print(f"Connecting to: {args.mongo_uri}")
    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    # Force server selection early.
    client.admin.command("ping")

    coll = client[args.db][args.coll]

    doc = {
        "seeded_at": time.time(),
        "name": name,
        "size": len(data),
        "sha1": sha1hex,
        args.field: Binary(data),
    }

    res = coll.insert_one(doc)
    print(f"Inserted _id: {res.inserted_id}")

    got = coll.find_one({"_id": res.inserted_id})
    if not got:
        print("ERROR: find_one returned no document")
        return 2

    field_val = got.get(args.field)
    print(f"Stored field {args.field!r} value type: {type(field_val)}")
    print(f"Expected sha1: {sha1hex}")

    # When BLOB offload is enabled, MoG replaces binary wrappers with:
    # {"__mog_blob__": {"table": <table>, "sha1": <sha1>, "len": <n>, "kind": <subtype>}}
    blob_ptr = None
    if isinstance(field_val, dict) and "__mog_blob__" in field_val and isinstance(field_val["__mog_blob__"], dict):
        blob_ptr = field_val["__mog_blob__"]

    if not blob_ptr:
        print("BLOB pointer not found in stored document.")
        # Try to show what we did get back.
        try:
            raw = bytes(field_val) if field_val is not None else b""
            if raw:
                print(f"Field appears to still be inline binary (len={len(raw)} sha1={_sha1_hex(raw)})")
        except Exception:
            pass

        print("Common causes:")
        print("- MoG server started without `MOG_BLOB_TABLE` (must be non-empty).")
        print("- Payload smaller than `MOG_BLOB_MIN_BYTES` on the server (default 256).")
        print("- Insert path didn’t see binary as BSON binary (fixed in recent MoG; rebuild/restart the server).")
        print("- MoG can’t reach MonkDB HTTP from where it runs.")
        print("  - If MoG runs in Docker on macOS and MonkDB runs on the host, use:")
        print("    `MOG_BLOB_HTTP_BASE=http://host.docker.internal:6000`")
        return 3

    print("BLOB pointer:", blob_ptr)
    print("")
    print("MonkDB blob endpoint commands:")
    print(f'  curl -X GET "{args.blob_http_base.rstrip("/")}/_blobs/{args.blob_table}/{sha1hex}" -o downloaded.bin')
    print(f'  curl -X DELETE "{args.blob_http_base.rstrip("/")}/_blobs/{args.blob_table}/{sha1hex}"')

    if args.verify_http:
        url = f'{args.blob_http_base.rstrip("/")}/_blobs/{args.blob_table}/{sha1hex}'
        try:
            downloaded = _http_get_bytes(url, args.timeout_s)
        except urllib.error.URLError as e:
            print(f"ERROR: HTTP verify failed GET {url}: {e}")
            return 4
        if downloaded != data:
            print(f"ERROR: downloaded bytes mismatch: got={len(downloaded)} expected={len(data)}")
            return 5
        print("HTTP verify: OK (downloaded bytes match)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
