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

    Uses the standard azure-storage-blob SDK connecting to the edge
    blob storage module, and adlfs for Zarr I/O.
    """

    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING", ""
        )
        if not self.connection_string:
            raise EnvironmentError(
                "AZURE_STORAGE_CONNECTION_STRING not set for AzureBlobEdgeStorage"
            )
        self._fs = None

    @property
    def fs(self):
        if self._fs is None:
            from adlfs import AzureBlobFileSystem
            self._fs = AzureBlobFileSystem(connection_string=self.connection_string)
        return self._fs

    def _ensure_container(self, container: str) -> None:
        from azure.storage.blob import BlobServiceClient
        client = BlobServiceClient.from_connection_string(self.connection_string)
        cc = client.get_container_client(container)
        if not cc.exists():
            cc.create_container()
            logger.info("Created container: %s", container)

    def save_zarr(self, dataset: xr.Dataset, path: str, mode: str = "w") -> str:
        container = path.split("/")[0]
        self._ensure_container(container)
        for var in dataset.data_vars:
            dataset[var].encoding.clear()
        for coord in dataset.coords:
            dataset[coord].encoding.clear()
        store = self.fs.get_mapper(path)
        dataset.to_zarr(store=store, mode=mode)
        logger.info("Saved Zarr to blob: %s", path)
        return path

    def append_zarr(self, dataset: xr.Dataset, path: str, append_dim: str = "ping_time") -> None:
        for var in dataset.data_vars:
            dataset[var].encoding.clear()
        for coord in dataset.coords:
            dataset[coord].encoding.clear()
        store = self.fs.get_mapper(path)
        dataset.to_zarr(store=store, mode="a", append_dim=append_dim)
        logger.info("Appended to blob: %s", path)

    def load_zarr(self, path: str, **kwargs) -> xr.Dataset:
        store = self.fs.get_mapper(path)
        return xr.open_zarr(store, **kwargs)

    def save_file(self, data: bytes, path: str) -> str:
        container = path.split("/")[0]
        self._ensure_container(container)
        blob_path = "/".join(path.split("/")[1:])
        from azure.storage.blob import BlobServiceClient
        client = BlobServiceClient.from_connection_string(self.connection_string)
        bc = client.get_blob_client(container=container, blob=blob_path)
        bc.upload_blob(data, overwrite=True)
        logger.info("Uploaded file to blob: %s", path)
        return path

    def exists(self, path: str) -> bool:
        return self.fs.exists(path)

    def list_stores(self, prefix: str) -> list[str]:
        try:
            items = self.fs.ls(prefix, detail=False)
            return [i for i in items if i.endswith(".zarr")]
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
