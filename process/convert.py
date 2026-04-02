"""Raw file → EchoData conversion for edge processing.

Supports two paths:
- File-based: ``open_raw()`` on a ``.raw`` file
- Ping-based: ``PingAccumulator.to_echodata()`` from real-time UDP data
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from echopype.echodata.echodata import EchoData

logger = logging.getLogger("oceanstream")


def convert_raw_file(
    raw_file_path: str,
    sonar_model: str = "EK80",
) -> "EchoData":
    """Convert a raw file to EchoData using echopype.

    Parameters
    ----------
    raw_file_path
        Path to the raw echosounder file.
    sonar_model
        Echosounder model string (``"EK80"``, ``"EK60"``).

    Returns
    -------
    EchoData
    """
    from echopype.convert.api import open_raw

    logger.info("Converting %s (model=%s)", raw_file_path, sonar_model)
    echodata = open_raw(str(raw_file_path), sonar_model=sonar_model)

    if echodata.beam is None:
        logger.warning("No beam data in %s", raw_file_path)

    return echodata


def save_echodata_zarr(
    echodata: "EchoData",
    zarr_path: str,
    overwrite: bool = True,
) -> str:
    """Save EchoData to a local Zarr store.

    Returns the zarr path.
    """
    logger.info("Saving EchoData to %s", zarr_path)
    echodata.to_zarr(save_path=zarr_path, overwrite=overwrite)
    return zarr_path
