"""Daily Zarr store manager for edge processing.

Manages one Sv Zarr store per calendar day.  Both real-time UDP pings
and file-based raw conversions write to the same daily store via
``append_sv()``.

Store layout::

    {base_path}/
      2026-04-02/
        sv.zarr           # accumulated Sv for this day
        sv_denoised.zarr  # denoised product (written periodically)
        mvbs.zarr
        nasc.zarr
"""

from __future__ import annotations

import gc
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import xarray as xr

from azure_handler.storage import StorageBackend

logger = logging.getLogger("oceanstream")


class DayStore:
    """Manage daily Zarr stores for incremental Sv accumulation.

    Parameters
    ----------
    storage : StorageBackend
        Storage backend to use for persistence.
    container : str
        Container or base prefix for processed data.
    """

    def __init__(self, storage: StorageBackend, container: str = "processed"):
        self.storage = storage
        self.container = container

    def _sv_path(self, day: date) -> str:
        """Return the Zarr path for a day's Sv store."""
        return f"{self.container}/{day.isoformat()}/sv.zarr"

    def _product_path(self, day: date, product: str) -> str:
        return f"{self.container}/{day.isoformat()}/{product}.zarr"

    def day_exists(self, day: date) -> bool:
        """Check if a Sv store exists for the given day."""
        return self.storage.exists(self._sv_path(day))

    def append_sv(self, ds_sv: xr.Dataset, day: Optional[date] = None) -> str:
        """Append Sv data to the daily store, creating it if needed.

        Parameters
        ----------
        ds_sv
            Sv dataset to append. Must have a ``ping_time`` dimension.
        day
            Calendar day for the store. If ``None``, derived from the
            first ``ping_time`` value.

        Returns
        -------
        str
            Path to the daily Sv Zarr store.
        """
        if day is None:
            first_time = ds_sv["ping_time"].values[0]
            day = _to_date(first_time)

        path = self._sv_path(day)

        if self.storage.exists(path):
            logger.info(
                "Appending %d pings to existing day store %s",
                ds_sv.sizes.get("ping_time", 0),
                path,
            )
            self.storage.append_zarr(ds_sv, path, append_dim="ping_time")
        else:
            logger.info(
                "Creating new day store %s with %d pings",
                path,
                ds_sv.sizes.get("ping_time", 0),
            )
            self.storage.save_zarr(ds_sv, path, mode="w")

        return path

    def load_sv(self, day: date, **kwargs) -> xr.Dataset:
        """Load the Sv store for a given day.

        Returns an empty Dataset if no data exists for that day.
        """
        path = self._sv_path(day)
        if not self.storage.exists(path):
            logger.warning("No Sv data for %s", day)
            return xr.Dataset()
        return self.storage.load_zarr(path, **kwargs)

    def save_product(
        self,
        ds: xr.Dataset,
        day: date,
        product: str,
    ) -> str:
        """Save a derived product (denoised, mvbs, nasc) for a day.

        Overwrites any existing product store for that day.

        Returns the product Zarr path.
        """
        path = self._product_path(day, product)
        self.storage.save_zarr(ds, path, mode="w")
        logger.info("Saved %s for %s → %s", product, day, path)
        return path

    def load_product(self, day: date, product: str, **kwargs) -> xr.Dataset:
        """Load a derived product for a given day."""
        path = self._product_path(day, product)
        if not self.storage.exists(path):
            return xr.Dataset()
        return self.storage.load_zarr(path, **kwargs)

    def list_days(self) -> list[date]:
        """List all days that have Sv data."""
        stores = self.storage.list_stores(self.container)
        days = []
        for s in stores:
            # Parse date from path like "processed/2026-04-02/sv.zarr"
            parts = Path(s).parts
            for part in parts:
                try:
                    days.append(date.fromisoformat(part))
                    break
                except ValueError:
                    continue
        return sorted(set(days))


def _to_date(val) -> date:
    """Convert numpy datetime64 or similar to ``datetime.date``."""
    import numpy as np
    import pandas as pd

    if isinstance(val, (np.datetime64, pd.Timestamp)):
        return pd.Timestamp(val).date()
    if isinstance(val, datetime):
        return val.date()
    return date.today()
