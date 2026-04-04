"""Bridge EdgeConfig flat fields to oceanstream echodata config dataclasses."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import EdgeConfig

from oceanstream.echodata.config import DenoiseConfig, MVBSConfig, NASCConfig


def to_denoise_config(cfg: "EdgeConfig") -> DenoiseConfig:
    """Convert edge config denoise fields to a ``DenoiseConfig``."""
    methods = [m.strip() for m in cfg.denoise_methods.split(",") if m.strip()]
    ping_lags = [int(x.strip()) for x in cfg.impulse_ping_lags.split(",") if x.strip()]

    noise_max = cfg.background_noise_max if cfg.background_noise_max > -999.0 else None
    pulse_length = cfg.denoise_pulse_length if cfg.denoise_pulse_length else None

    return DenoiseConfig(
        methods=methods,
        use_frequency_specific=cfg.denoise_use_frequency_specific,
        pulse_length=pulse_length,
        # Background noise
        background_num_side_pings=cfg.background_num_side_pings,
        background_range_window=cfg.background_range_window,
        background_ping_window=cfg.background_ping_window,
        background_snr_threshold=cfg.background_snr_threshold,
        background_noise_max=noise_max,
        # Transient noise
        transient_a=cfg.transient_a,
        transient_n=cfg.transient_n,
        transient_exclude_above=cfg.transient_exclude_above,
        transient_depth_bin=cfg.transient_depth_bin,
        transient_n_pings=cfg.transient_n_pings,
        transient_threshold_db=cfg.transient_threshold_db,
        # Impulse noise
        impulse_threshold_db=cfg.impulse_threshold_db,
        impulse_num_lags=cfg.impulse_num_lags,
        impulse_vertical_bin=cfg.impulse_vertical_bin,
        impulse_ping_lags=ping_lags,
        # Attenuation
        attenuation_threshold=cfg.attenuation_threshold,
        attenuation_upper_limit=cfg.attenuation_upper_limit,
        attenuation_lower_limit=cfg.attenuation_lower_limit,
        attenuation_side_pings=cfg.attenuation_side_pings,
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
