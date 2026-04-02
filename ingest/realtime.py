"""Real-time UDP ingestion via the EK80 legacy client.

Connects to the EK80 echosounder, subscribes to SampleData (power)
for each channel, buffers pings using ``PingAccumulator``, and hands
off batches to the processing pipeline at configurable intervals.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable, Optional

import numpy as np

if TYPE_CHECKING:
    from config import EdgeConfig
    from process.day_store import DayStore

logger = logging.getLogger("oceanstream")

# Minimum pings before we trigger a processing batch
_MIN_PINGS_PER_BATCH = 5


class RealtimeIngestion:
    """Async service for real-time EK80 UDP data acquisition.

    Parameters
    ----------
    config : EdgeConfig
        Edge processing configuration.
    on_batch : callable
        Async callback invoked with ``(EchoData, config)`` when a batch
        buffer is full.  Typically the pipeline orchestrator.
    """

    def __init__(
        self,
        config: "EdgeConfig",
        on_batch: Callable,
    ):
        self.config = config
        self.on_batch = on_batch
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the real-time ingestion loop."""
        if self._running:
            logger.warning("Realtime ingestion already running")
            return
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info(
            "Realtime ingestion started (host=%s, port=%d, buffer=%ds)",
            self.config.ek80_host,
            self.config.ek80_port,
            self.config.realtime_buffer_seconds,
        )

    async def stop(self) -> None:
        """Gracefully stop the ingestion loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Realtime ingestion stopped")

    async def _run(self) -> None:
        """Main ingestion loop: connect → subscribe → buffer → process."""
        from ek80_udp_client import (
            EK80LegacyClient,
            decode_sample_power,
            decode_bottom_detection,
        )
        from echopype.convert.from_ping_data import PingAccumulator, ChannelConfig

        while self._running:
            try:
                await self._run_session(EK80LegacyClient, PingAccumulator, ChannelConfig)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Realtime session failed: %s — retrying in 10s", e, exc_info=True)
                await asyncio.sleep(10)

    async def _run_session(self, ClientClass, AccumulatorClass, ChannelConfigClass) -> None:
        """One connection session — reconnects on failure."""
        host = self.config.ek80_host
        port = self.config.ek80_port

        async with ClientClass(host, port=port) as client:
            # Phase 1: Discover
            info = await client.request_server_info()
            logger.info("Connected to %s (%s)", info.host_name, info.application_name)
            await client.connect()

            # Phase 2: Discover channels (may be empty if not pinging)
            channels_str = await client.get_parameter("TransceiverMgr/Channels")
            if not channels_str:
                logger.warning("No channels available — waiting for EK80 to start pinging")
                for _ in range(30):  # retry for ~5 minutes
                    await asyncio.sleep(10)
                    channels_str = await client.get_parameter("TransceiverMgr/Channels")
                    if channels_str:
                        break
                if not channels_str:
                    logger.error("No channels after retries — aborting session")
                    return

            channel_list = [ch.strip() for ch in channels_str.split(";") if ch.strip()]
            logger.info("Discovered %d channels: %s", len(channel_list), channel_list)

            # Phase 3: Query per-channel calibration/transmit parameters
            # These are static during a ping session — query once before subscribing
            ch_params: dict[str, dict[str, float | str]] = {}
            for ch_id in channel_list:
                ch_params[ch_id] = await _query_channel_params(client, ch_id)
                logger.info(
                    "Channel %s: freq=%.0f Hz, Tx=%.0f W, PD=%.6f s, SI=%.6f s",
                    ch_id,
                    ch_params[ch_id]["frequency"],
                    ch_params[ch_id]["transmit_power"],
                    ch_params[ch_id]["pulse_duration"],
                    ch_params[ch_id]["sample_interval"],
                )

            # Phase 4: Get navigation data
            try:
                lat = float(await client.get_parameter("OwnShip/Latitude") or 0)
                lon = float(await client.get_parameter("OwnShip/Longitude") or 0)
            except (ValueError, TypeError):
                lat, lon = 0.0, 0.0

            # Phase 5: Setup accumulator with full channel config
            accumulator = AccumulatorClass(sonar_model=self.config.sonar_model)

            for ch_id in channel_list:
                p = ch_params[ch_id]
                accumulator.register_channel(ChannelConfigClass(
                    channel_id=ch_id,
                    frequency=p["frequency"],
                    pulse_duration=p["pulse_duration"],
                    sample_interval=p["sample_interval"],
                    gain=p["gain"],
                    sa_correction=p["sa_correction"],
                    equivalent_beam_angle=p["equivalent_beam_angle"],
                    beam_width_alongship=p["beam_width_alongship"],
                    beam_width_athwartship=p["beam_width_athwartship"],
                    transceiver_type=p["transceiver_type"],
                ))

            # Phase 6: Subscribe to SampleData for each channel
            subscriptions = {}
            for ch_id in channel_list:
                try:
                    sub_id, data_port = await client.subscribe_sample_data(
                        channel_id=ch_id, sample_type="Power"
                    )
                    subscriptions[sub_id] = ch_id
                    logger.info("Subscribed SampleData for %s (sub=%d, port=%d)", ch_id, sub_id, data_port)
                except Exception as e:
                    logger.error("Failed to subscribe %s: %s", ch_id, e)

            if not subscriptions:
                logger.error("No subscriptions established — aborting")
                return

            # Phase 6: Buffer loop
            buffer_timeout = self.config.realtime_buffer_seconds
            batch_start = asyncio.get_event_loop().time()

            async for prd in client.iter_data_reassembled(timeout=float(buffer_timeout + 30)):
                if not self._running:
                    break

                ch_id = subscriptions.get(prd.subscription_id)
                if ch_id is None:
                    continue

                # Decode the sample data
                try:
                    from ek80_udp_client import decode_sample_power
                    sample = decode_sample_power(prd.payload)
                    p = ch_params[ch_id]

                    accumulator.add_ping(
                        timestamp=sample.time,
                        channel_id=ch_id,
                        power_samples=np.array(sample.samples, dtype=np.int16),
                        transmit_power=p["transmit_power"],
                        pulse_duration=p["pulse_duration"],
                        sample_interval=p["sample_interval"],
                        frequency=p["frequency"],
                        sound_speed=p["sound_speed"],
                        absorption=p["absorption"],
                    )
                except Exception as e:
                    logger.debug("Failed to decode ping for %s: %s", ch_id, e)
                    continue

                # Periodically poll navigation
                elapsed = asyncio.get_event_loop().time() - batch_start
                if elapsed >= 5.0:
                    try:
                        lat = float(await client.get_parameter("OwnShip/Latitude") or lat)
                        lon = float(await client.get_parameter("OwnShip/Longitude") or lon)
                        heading = float(await client.get_parameter("OwnShip/Heading") or 0)
                        accumulator.add_navigation(
                            timestamp=datetime.now(timezone.utc),
                            latitude=lat,
                            longitude=lon,
                            heading=heading,
                        )
                    except Exception:
                        pass

                # Check if buffer interval reached
                if accumulator.duration_seconds >= buffer_timeout and len(accumulator) >= _MIN_PINGS_PER_BATCH:
                    logger.info(
                        "Buffer full: %d pings, %.1fs — triggering processing",
                        len(accumulator),
                        accumulator.duration_seconds,
                    )
                    try:
                        echodata = accumulator.to_echodata()
                        await self.on_batch(echodata, self.config)
                    except Exception as e:
                        logger.error("Batch processing failed: %s", e, exc_info=True)
                    finally:
                        accumulator.clear()
                        batch_start = asyncio.get_event_loop().time()

            # Flush remaining data
            if len(accumulator) >= _MIN_PINGS_PER_BATCH:
                logger.info("Flushing %d remaining pings", len(accumulator))
                try:
                    echodata = accumulator.to_echodata()
                    await self.on_batch(echodata, self.config)
                except Exception as e:
                    logger.error("Final batch failed: %s", e, exc_info=True)

            # Cleanup subscriptions
            for sub_id in subscriptions:
                try:
                    await client.unsubscribe(sub_id)
                except Exception:
                    pass


def _parse_frequency_from_channel_id(ch_id: str) -> float:
    """Extract frequency from channel ID like 'WBT 1-1 ES200-7C'.

    Looks for patterns like ES200 (200 kHz), ES70 (70 kHz), ES38 (38 kHz).
    Falls back to 200 kHz if unrecognised.
    """
    import re

    match = re.search(r"ES(\d+)", ch_id)
    if match:
        return float(match.group(1)) * 1000.0  # kHz → Hz

    match = re.search(r"(\d+)\s*kHz", ch_id, re.IGNORECASE)
    if match:
        return float(match.group(1)) * 1000.0

    return 200_000.0  # default


def _parse_float(val: str | None, default: float = 0.0) -> float:
    """Safely parse a string to float."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


async def _query_channel_params(client, ch_id: str) -> dict[str, float | str]:
    """Query per-channel calibration and transmit parameters from the EK80.

    Uses GetParameter on ``TransceiverMgr/{ch_id}/{suffix}`` paths.
    Returns a dict with all values needed for ChannelConfig and add_ping().
    """
    suffixes = [
        "Frequency", "SampleInterval", "TransmitPower", "PulseLength",
        "SoundVelocity", "AbsorptionCoefficient",
        "Gain", "SaCorrection", "EquivalentBeamAngle",
        "BeamWidthAlongship", "BeamWidthAthwartship",
        "TransceiverType",
    ]
    raw: dict[str, str | None] = {}
    for s in suffixes:
        try:
            raw[s] = await client.get_parameter(f"TransceiverMgr/{ch_id}/{s}")
        except Exception:
            raw[s] = None

    freq = _parse_float(raw["Frequency"], _parse_frequency_from_channel_id(ch_id))
    return {
        "frequency": freq,
        "sample_interval": _parse_float(raw["SampleInterval"], 0.000016),
        "transmit_power": _parse_float(raw["TransmitPower"], 100.0),
        "pulse_duration": _parse_float(raw["PulseLength"], 0.001024),
        "sound_speed": _parse_float(raw["SoundVelocity"], 1500.0),
        "absorption": _parse_float(raw["AbsorptionCoefficient"], 0.0),
        "gain": _parse_float(raw["Gain"], 25.0),
        "sa_correction": _parse_float(raw["SaCorrection"], 0.0),
        "equivalent_beam_angle": _parse_float(raw["EquivalentBeamAngle"], -20.7),
        "beam_width_alongship": _parse_float(raw["BeamWidthAlongship"], 7.0),
        "beam_width_athwartship": _parse_float(raw["BeamWidthAthwartship"], 7.0),
        "transceiver_type": raw.get("TransceiverType") or "WBT",
    }
