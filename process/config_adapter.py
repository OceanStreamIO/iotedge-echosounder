"""Bridge EdgeConfig flat fields to oceanstream echodata config dataclasses."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import EdgeConfig

from oceanstream.echodata.config import DenoiseConfig, MVBSConfig, NASCConfig


def to_denoise_config(cfg: "EdgeConfig") -> DenoiseConfig:
    """Convert edge config denoise fields to a ``DenoiseConfig``."""
    methods = [m.strip() for m in cfg.denoise_methods.split(",") if m.strip()]
    return DenoiseConfig(
        methods=methods,
        use_frequency_specific=cfg.denoise_use_frequency_specific,
    )


def to_mvbs_config(cfg: "EdgeConfig") -> MVBSConfig:
    """Convert edge config MVBS fields to a ``MVBSConfig``."""
    return MVBSConfig(
        range_bin=cfg.mvbs_range_bin,
        ping_time_bin=cfg.mvbs_ping_time_bin,
    )


def to_nasc_config(cfg: "EdgeConfig") -> NASCConfig:
    """Convert edge config NASC fields to a ``NASCConfig``."""
    return NASCConfig(
        range_bin=cfg.nasc_range_bin,
        dist_bin=cfg.nasc_dist_bin,
    )
