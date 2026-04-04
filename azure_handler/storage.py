"""Storage abstraction for edge data persistence.

Supports three backends:
- ``azure-blob-edge``: Azure Blob Storage on IoT Edge (localhost:11002)
- ``minio``: MinIO object storage (local or networked)
- ``local``: Direct filesystem writes (always works, no sync)

Auto-detection tries Azure Blob Edge first, falls back to local.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import xarray as xr

logger = logging.getLogger("oceanstream")


class StorageBackend(ABC):
    """Protocol for edge data storage."""

    @abstractmethod
    def save_zarr(self, dataset: xr.Dataset, path: str, mode: str = "w") -> str:
        """Save an xarray Dataset as Zarr. Returns the resolved path."""

    @abstractmethod
    def append_zarr(self, dataset: xr.Dataset, path: str, append_dim: str = "ping_time") -> None:
        """Append to an existing Zarr store along a dimension."""

    @abstractmethod
    def load_zarr(self, path: str, **kwargs) -> xr.Dataset:
        """Load a Zarr store as an xarray Dataset."""

    @abstractmethod
    def save_file(self, data: bytes, path: str) -> str:
        """Save raw bytes to a file path. Returns the resolved path."""

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a path exists in the store."""

    @abstractmethod
    def list_stores(self, prefix: str) -> list[str]:
        """List Zarr stores under a prefix."""


class LocalStorage(StorageBackend):
    """Direct filesystem storage."""

    def __init__(self, base_path: str = "/app/processed"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _resolve(self, path: str) -> Path:
        full = self.base_path / path
        full.parent.mkdir(parents=True, exist_ok=True)
        return full

    def save_zarr(self, dataset: xr.Dataset, path: str, mode: str = "w") -> str:
        full = self._resolve(path)
        for var in dataset.data_vars:
            dataset[var].encoding.clear()
        for coord in dataset.coords:
            dataset[coord].encoding.clear()
        dataset.to_zarr(str(full), mode=mode)
        logger.info("Saved Zarr to %s", full)
        return str(full)

    def append_zarr(self, dataset: xr.Dataset, path: str, append_dim: str = "ping_time") -> None:
        full = self._resolve(path)
        for var in dataset.data_vars:
            dataset[var].encoding.clear()
        for coord in dataset.coords:
            dataset[coord].encoding.clear()
        dataset.to_zarr(str(full), mode="a", append_dim=append_dim)
        logger.info("Appended %d records to %s", dataset.sizes.get(append_dim, 0), full)

    def load_zarr(self, path: str, **kwargs) -> xr.Dataset:
        full = self.base_path / path
        return xr.open_zarr(str(full), **kwargs)

    def save_file(self, data: bytes, path: str) -> str:
        full = self._resolve(path)
        full.write_bytes(data)
        return str(full)

    def exists(self, path: str) -> bool:
        return (self.base_path / path).exists()

    def list_stores(self, prefix: str) -> list[str]:
        base = self.base_path / prefix
        if not base.exists():
            return []
        return [
            str(p.relative_to(self.base_path))
            for p in base.rglob("*.zarr")
            if p.is_dir()
        ]


class AzureBlobEdgeStorage(StorageBackend):
    """Azure Blob Storage on IoT Edge (via localhost:11002).

    Uses ``azure-storage-blob`` SDK directly with a pinned API version
    (``2019-07-07``) that the edge blob module supports.  The ``adlfs``
    library sends newer API headers that the emulator rejects, so we
    avoid it entirely and implement a thin ``MutableMapping`` wrapper
    for Zarr I/O.
    """

    # Edge blob storage supports up to 2019-07-07
    _API_VERSION = "2019-07-07"

    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING", ""
        )
        if not self.connection_string:
            raise EnvironmentError(
                "AZURE_STORAGE_CONNECTION_STRING not set for AzureBlobEdgeStorage"
            )
        self._client = None

    @property
    def client(self):
        if self._client is None:
            from azure.storage.blob import BlobServiceClient
            self._client = BlobServiceClient.from_connection_string(
                self.connection_string, api_version=self._API_VERSION,
            )
        return self._client

    def _ensure_container(self, container: str) -> None:
        cc = self.client.get_container_client(container)
        try:
            cc.get_container_properties()
        except Exception:
            cc.create_container()
            logger.info("Created container: %s", container)

    def save_zarr(self, dataset: xr.Dataset, path: str, mode: str = "w") -> str:
        for var in dataset.data_vars:
            dataset[var].encoding.clear()
        for coord in dataset.coords:
            dataset[coord].encoding.clear()
        container = path.split("/")[0]
        prefix = "/".join(path.split("/")[1:])
        self._ensure_container(container)
        with tempfile.TemporaryDirectory() as tmp:
            local_path = os.path.join(tmp, "store.zarr")
            dataset.to_zarr(local_path, mode=mode)
            self._upload_dir(container, prefix, local_path)
        logger.info("Saved Zarr to blob: %s", path)
        return path

    def append_zarr(self, dataset: xr.Dataset, path: str, append_dim: str = "ping_time") -> None:
        for var in dataset.data_vars:
            dataset[var].encoding.clear()
        for coord in dataset.coords:
            dataset[coord].encoding.clear()
        container = path.split("/")[0]
        prefix = "/".join(path.split("/")[1:])
        self._ensure_container(container)
        with tempfile.TemporaryDirectory() as tmp:
            local_path = os.path.join(tmp, "store.zarr")
            # Download existing store first so xarray can append
            self._download_dir(container, prefix, local_path)
            dataset.to_zarr(local_path, mode="a", append_dim=append_dim)
            self._upload_dir(container, prefix, local_path)
        logger.info("Appended to blob: %s", path)

    def load_zarr(self, path: str, **kwargs) -> xr.Dataset:
        container = path.split("/")[0]
        prefix = "/".join(path.split("/")[1:])
        tmp = tempfile.mkdtemp()
        local_path = os.path.join(tmp, "store.zarr")
        self._download_dir(container, prefix, local_path)
        return xr.open_zarr(local_path, **kwargs)

    def _upload_dir(self, container: str, prefix: str, local_path: str) -> None:
        """Upload all files in a local directory to blob storage."""
        cc = self.client.get_container_client(container)
        for root, _dirs, files in os.walk(local_path):
            for fname in files:
                fpath = os.path.join(root, fname)
                rel = os.path.relpath(fpath, local_path)
                blob_name = f"{prefix}/{rel}" if prefix else rel
                bc = cc.get_blob_client(blob_name)
                with open(fpath, "rb") as f:
                    bc.upload_blob(f, overwrite=True)

    def _download_dir(self, container: str, prefix: str, local_path: str) -> None:
        """Download all blobs under a prefix to a local directory."""
        cc = self.client.get_container_client(container)
        blob_prefix = prefix.rstrip("/") + "/"
        os.makedirs(local_path, exist_ok=True)
        for blob in cc.list_blobs(name_starts_with=blob_prefix):
            rel = blob.name[len(blob_prefix):]
            dest = os.path.join(local_path, rel)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            bc = cc.get_blob_client(blob.name)
            with open(dest, "wb") as f:
                f.write(bc.download_blob().readall())

    def save_file(self, data: bytes, path: str) -> str:
        container = path.split("/")[0]
        self._ensure_container(container)
        blob_path = "/".join(path.split("/")[1:])
        bc = self.client.get_blob_client(container=container, blob=blob_path)
        bc.upload_blob(data, overwrite=True)
        logger.info("Uploaded file to blob: %s", path)
        return path

    def exists(self, path: str) -> bool:
        parts = path.split("/")
        container = parts[0]
        prefix = "/".join(parts[1:])
        cc = self.client.get_container_client(container)
        try:
            cc.get_container_properties()
        except Exception:
            return False
        # Check if it's a blob or a virtual directory (has children)
        try:
            bc = cc.get_blob_client(prefix)
            bc.get_blob_properties()
            return True
        except Exception:
            pass
        # Check if any blobs exist under this prefix
        blobs = list(cc.list_blobs(name_starts_with=prefix + "/", results_per_page=1))
        return len(blobs) > 0

    def list_stores(self, prefix: str) -> list[str]:
        parts = prefix.split("/")
        container = parts[0]
        blob_prefix = "/".join(parts[1:]) if len(parts) > 1 else ""
        cc = self.client.get_container_client(container)
        try:
            blobs = cc.list_blobs(name_starts_with=blob_prefix)
            # Find unique .zarr directories
            stores = set()
            for b in blobs:
                name = b.name
                idx = name.find(".zarr/")
                if idx >= 0:
                    stores.add(f"{container}/{name[:idx + 5]}")
            return sorted(stores)
        except Exception:
            return []


def create_storage(backend: str = "azure-blob-edge", **kwargs) -> StorageBackend:
    """Factory to create the appropriate storage backend.

    Tries ``azure-blob-edge`` first.  If it fails (e.g. connection
    string missing or edge blob module unavailable), falls back to
    ``local``.
    """
    base_path = kwargs.get("base_path", "/app/processed")
    if backend == "azure-blob-edge":
        try:
            return AzureBlobEdgeStorage(
                connection_string=kwargs.get("connection_string"),
            )
        except Exception as e:
            logger.warning("Azure Blob Edge unavailable (%s), falling back to local storage", e)
            return LocalStorage(base_path)
    elif backend == "minio":
        # MinIO uses S3-compatible interface — same pattern as Azure but
        # with different endpoint. Placeholder for future implementation.
        logger.warning("MinIO backend not yet implemented, falling back to local storage")
        return LocalStorage(base_path)
    else:
        return LocalStorage(base_path)
