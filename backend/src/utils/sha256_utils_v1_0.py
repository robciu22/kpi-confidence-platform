#!/usr/bin/env python3
"""
src/utils/sha256_utils_v1_0.py

Streaming-SHA256 + Fingerprint-Hilfsfunktion für große Dateien.
Hasht die Datei wie gespeichert (komprimiertes tgz/csv.gz), was für die Änderungserkennung korrekt ist.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FileFingerprint:
    path: str
    size_bytes: int
    sha256: str
    mtime_utc: float  # Epochensekunden


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def sha256_file(path: str | Path, *, chunk_size_bytes: int = 8 * 1024 * 1024) -> str:
    p = Path(path).expanduser().resolve()
    h = hashlib.sha256()

    with p.open("rb") as f:
        while True:
            chunk = f.read(chunk_size_bytes)
            if not chunk:
                break
            h.update(chunk)

    return h.hexdigest()


def fingerprint_file(
    path: str | Path, *, chunk_size_bytes: int = 8 * 1024 * 1024
) -> FileFingerprint:
    p = Path(path).expanduser().resolve()
    st = p.stat()
    return FileFingerprint(
        path=str(p),
        size_bytes=int(st.st_size),
        sha256=sha256_file(p, chunk_size_bytes=chunk_size_bytes),
        mtime_utc=float(st.st_mtime),
    )
