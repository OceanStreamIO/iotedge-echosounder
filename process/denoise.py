"""Noise removal for edge Sv datasets.

Wraps ``echopype.clean.remove_background_noise`` with GPU acceleration
and provides a single ``denoise()`` entry point.
"""

from __future__ import annotations

import logging

import xarray as xr

logger = logging.getLogger("oceanstream")


def denoise(
    ds_sv: xr.Dataset,
    *,
    ping_num: int = 20,
    range_sample_num: int = 20,
    background_noise_max: str | None = None,
    SNR_threshold: str = "3.0dB",
    use_gpu: bool = True,
) -> xr.Dataset:
    """Remove background noise from Sv dataset.

    Parameters
    ----------
    ds_sv
        Sv dataset (output of ``compute_sv``).
    ping_num
        Number of pings for noise estimation window.
    range_sample_num
        Number of range samples for noise estimation window.
    background_noise_max
        Maximum noise level (e.g. ``"-125dB"``). ``None`` disables cap.
    SNR_threshold
        Signal-to-noise ratio threshold (e.g. ``"3.0dB"``).
    use_gpu
        Whether to use GPU acceleration.

    Returns
    -------
    xr.Dataset
        Denoised Sv dataset.
    """
    from echopype.clean import remove_background_noise

    gpu_flag = "auto" if use_gpu else False

    logger.info(
        "Denoising (ping_num=%d, range_sample_num=%d, SNR=%s, gpu=%s)",
        ping_num, range_sample_num, SNR_threshold, gpu_flag,
    )

    ds_denoised = remove_background_noise(
        ds_sv,
        ping_num=ping_num,
        range_sample_num=range_sample_num,
        background_noise_max=background_noise_max,
        SNR_threshold=SNR_threshold,
        use_gpu=gpu_flag,
    )

    logger.info("Denoising complete")
    return ds_denoised
