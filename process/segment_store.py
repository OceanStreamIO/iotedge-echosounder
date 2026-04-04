"""Segment-based store for edge processing.

Each processing batch is saved as an independent segment with its own
subfolder.  Segments are named by their time interval
(``YYYYMMDDTHHMMSS_YYYYMMDDTHHMMSS``) and contain all products for
that batch: ``sv.zarr``, ``sv_denoised.zarr``, ``mvbs.zarr``,
``nasc.zarr``, ``echograms/``, and a ``metadata.json`` manifest.

Layout::

    {container}/
      2026-04-03/
        segments/
          14-32-00__14-35-13/
            sv.zarr
            sv_denoised.zarr
            mvbs.zarr
            nasc.zarr
            echograms/
              sv_ch1.png
              denoised_ch1.png
              mvbs.png
            metadata.json
          14-35-13__14-38-26/
            ...

A separate consolidation job (not part of this module) can later
merge segments into daily products and optionally delete segments.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import xarray as xr

from azure_handler.storage import StorageBackend

logger = logging.getLogger("oceanstream")


class SegmentStore:
    """Write independent processing segments to edge storage.

    Parameters
    ----------
    storage : StorageBackend
        Storage backend for persistence.
    container : str
        Container or base prefix for processed data.
    """

    def __init__(self, storage: StorageBackend, container: str = "processed"):
        self.storage = storage
        self.container = container

    @staticmethod
    def segment_name(ds: xr.Dataset) -> str:
        """Derive segment folder name from first/last ping_time."""
        times = ds["ping_time"].values
        t0 = pd.Timestamp(times[0])
        t1 = pd.Timestamp(times[-1])
        fmt = "%H-%M-%S"
        return f"{t0.strftime(fmt)}__{t1.strftime(fmt)}"

    @staticmethod
    def segment_day(ds: xr.Dataset) -> date:
        """Derive calendar day from the first ping_time."""
        return pd.Timestamp(ds["ping_time"].values[0]).date()

    def _segment_prefix(self, day: date, seg_name: str) -> str:
        return f"{self.container}/{day.isoformat()}/segments/{seg_name}"

    def save_zarr(
        self, ds: xr.Dataset, day: date, seg_name: str, product: str
    ) -> str:
        """Save a Zarr product inside a segment folder.

        Returns the full path.
        """
        path = f"{self._segment_prefix(day, seg_name)}/{product}.zarr"
        self.storage.save_zarr(ds, path, mode="w")
        logger.info("Saved %s → %s", product, path)
        return path

    def save_file(
        self, data: bytes, day: date, seg_name: str, filename: str
    ) -> str:
        """Save a file (e.g. PNG) inside a segment folder."""
        path = f"{self._segment_prefix(day, seg_name)}/{filename}"
        self.storage.save_file(data, path)
        return path

    def save_metadata(
        self, metadata: Dict[str, Any], day: date, seg_name: str
    ) -> str:
        """Save segment metadata.json."""
        path = f"{self._segment_prefix(day, seg_name)}/metadata.json"
        data = json.dumps(metadata, indent=2, default=str).encode("utf-8")
        self.storage.save_file(data, path)
        logger.info("Saved metadata → %s", path)
        return path

    def list_segments(self, day: date) -> list[str]:
        """List segment names for a given day."""
        prefix = f"{self.container}/{day.isoformat()}/segments"
        stores = self.storage.list_stores(prefix)
        names = set()
        for s in stores:
            parts = Path(s).parts
            try:
                seg_idx = list(parts).index("segments")
                if seg_idx + 1 < len(parts):
                    names.add(parts[seg_idx + 1])
            except ValueError:
                continue
        return sorted(names)

    def list_days(self) -> list[date]:
        """List all days that have at least one segment."""
        stores = self.storage.list_stores(self.container)
        days = set()
        for s in stores:
            parts = Path(s).parts
            for part in parts:
                try:
                    days.add(date.fromisoformat(part))
                    break
                except ValueError:
                    continue
        return sorted(days)
