"""
Thin artifact-store abstraction for ML model persistence.

v1: local filesystem backend.  All Phase 6 jobs use ``get_artifact_store()``
— never touch the filesystem directly.

Migrating to multi-host: flip runtime config ``model_store.backend`` to ``s3``,
implement ``S3CompatibleArtifactStore``, copy existing artifacts.  No code changes
in jobs.
"""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path
from typing import Any, NamedTuple, Protocol, runtime_checkable


class ArtifactHandle(NamedTuple):
    uri: str
    sha256: str
    size_bytes: int


class ArtifactNotFound(Exception):
    pass


class ArtifactIntegrityError(Exception):
    pass


@runtime_checkable
class ArtifactStore(Protocol):
    def put(self, key: str, data: bytes, *, content_type: str = "application/octet-stream") -> ArtifactHandle: ...
    def get(self, uri: str) -> bytes: ...
    def exists(self, uri: str) -> bool: ...
    def delete(self, uri: str) -> None: ...


_SAFE_KEY_RE = re.compile(r"^[a-zA-Z0-9_\-./]+$")


def _validate_key(key: str) -> None:
    if not key or ".." in key or key.startswith("/") or not _SAFE_KEY_RE.match(key):
        raise ValueError(f"Unsafe artifact key: {key!r}")


class LocalFilesystemArtifactStore:
    """Stores artifacts under a local root directory via atomic rename."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    def _path_for(self, key: str) -> Path:
        return self._root / key

    def _path_from_uri(self, uri: str) -> Path:
        if uri.startswith("file://"):
            return Path(uri[7:])
        return Path(uri)

    def put(self, key: str, data: bytes, *, content_type: str = "application/octet-stream") -> ArtifactHandle:
        _validate_key(key)
        target = self._path_for(key)
        target.parent.mkdir(parents=True, exist_ok=True)

        sha = hashlib.sha256(data).hexdigest()

        fd, tmp = tempfile.mkstemp(dir=str(target.parent), suffix=".tmp")
        try:
            os.write(fd, data)
            os.close(fd)
            os.replace(tmp, str(target))
        except BaseException:
            os.close(fd) if not os.get_inheritable(fd) else None
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

        uri = f"file://{target.resolve()}"
        return ArtifactHandle(uri=uri, sha256=sha, size_bytes=len(data))

    def get(self, uri: str) -> bytes:
        p = self._path_from_uri(uri)
        if not p.exists():
            raise ArtifactNotFound(f"Artifact not found: {uri}")
        return p.read_bytes()

    def exists(self, uri: str) -> bool:
        return self._path_from_uri(uri).exists()

    def delete(self, uri: str) -> None:
        p = self._path_from_uri(uri)
        if p.exists():
            p.unlink()


def get_artifact_store(cfg: dict[str, Any] | None = None) -> ArtifactStore:
    """Factory — returns the configured artifact store.

    Reads ``model_store.backend`` and ``model_store.root_uri`` from *cfg*.
    Defaults to local filesystem at ``/var/www/SupplyCore/var/models/spy_shadow/``.
    """
    cfg = cfg or {}
    ms = cfg.get("model_store", {})
    backend = ms.get("backend", "local")
    root = ms.get("root_uri", "/var/www/SupplyCore/var/models/spy_shadow")

    if backend == "local":
        return LocalFilesystemArtifactStore(root)

    raise ValueError(f"Unknown model_store.backend: {backend!r}  (only 'local' is implemented in v1)")
