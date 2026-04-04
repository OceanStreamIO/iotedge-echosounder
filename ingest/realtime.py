"""Real-time ingestion via the EK80 HTTP/WebSocket service.

Connects to the local ``ek80-service`` (FastAPI, typically on port 8050),
fetches channel calibration via REST, then subscribes to ``/ws/sample-data``
for full-resolution power samples.  Pings are buffered with
``PingAccumulator`` and handed off to the processing pipeline.

This avoids importing ``ek80_udp_client`` inside the Docker image — all
EK80 communication goes through the host-level service.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Optional

import numpy as np

if TYPE_CHECKING:
    from config import EdgeConfig

logger = logging.getLogger("oceanstream")

# Maximum concurrent background batch tasks.  With 500-ping batches and
# ~30 min buffers the processing usually finishes well before the next
# batch, but the semaphore prevents unbounded task growth if processing
# is slower than acquisition.
_MAX_CONCURRENT_BATCHES = 2

# Thread pool for CPU-heavy batch processing so it doesn't block the
# event loop (which must stay responsive for WebSocket keepalive).
from concurrent.futures import ThreadPoolExecutor
_BATCH_EXECUTOR = ThreadPoolExecutor(max_workers=_MAX_CONCURRENT_BATCHES, thread_name_prefix="batch")


class RealtimeIngestion:
    """Async service for real-time data acquisition from the ek80-service.

    Parameters
    ----------
    config : EdgeConfig
        Edge processing configuration (must have ``ek80_service_url``).
    on_batch : callable
        Async callback invoked with ``(EchoData, config)`` when a batch
        buffer is full.
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
        self._batch_sem = asyncio.Semaphore(_MAX_CONCURRENT_BATCHES)
        self._batch_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        """Start the real-time ingestion loop."""
        if self._running:
            logger.warning("Realtime ingestion already running")
            return
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info(
            "Realtime ingestion started (service=%s, buffer=%d pings / %ds, min_pings=%d)",
            self.config.ek80_service_url,
            self.config.realtime_buffer_pings,
            self.config.realtime_buffer_seconds,
            self.config.realtime_min_pings,
        )

    async def stop(self) -> None:
        """Gracefully stop the ingestion loop and drain pending batches."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        # Wait for any in-flight background batch tasks
        if self._batch_tasks:
            logger.info("Waiting for %d in-flight batch tasks…", len(self._batch_tasks))
            await asyncio.gather(*self._batch_tasks, return_exceptions=True)
            self._batch_tasks.clear()
        logger.info("Realtime ingestion stopped")

    async def _run(self) -> None:
        """Main ingestion loop with auto-reconnect."""
        while self._running:
            try:
                await self._run_session()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Realtime session failed: %s — retrying in 10s", e, exc_info=True)
                await asyncio.sleep(10)

    async def _run_session(self) -> None:
        """One connection session — reconnects on failure."""
        import websockets

        from echopype.convert.from_ping_data import PingAccumulator, ChannelConfig

        base_url = self.config.ek80_service_url.rstrip("/")

        # Phase 1: Wait for service health
        await self._wait_for_service(base_url)

        # Phase 2: Fetch channel calibration via REST
        channels = await self._fetch_channels(base_url)
        if not channels:
            logger.error("No channels available from ek80-service — aborting session")
            return

        logger.info("Discovered %d channels from ek80-service", len(channels))
        for ch in channels:
            logger.info(
                "  %s: freq=%.0f Hz, Tx=%.0f W, PD=%.6f s, SI=%.6f s",
                ch["channel_id"],
                ch.get("frequency", 0),
                ch.get("transmit_power", 0),
                ch.get("pulse_length", 0),
                ch.get("sample_interval", 0),
            )

        # Phase 3: Fetch navigation
        nav = await self._fetch_navigation(base_url)
        lat, lon = nav.get("latitude", 0.0) or 0.0, nav.get("longitude", 0.0) or 0.0

        # Phase 4: Setup accumulator
        accumulator = PingAccumulator(sonar_model=self.config.sonar_model)
        ch_lookup: dict[str, dict[str, Any]] = {}

        for ch in channels:
            ch_id = ch["channel_id"]
            ch_lookup[ch_id] = ch
            accumulator.register_channel(ChannelConfig(
                channel_id=ch_id,
                frequency=ch.get("frequency", 200000.0),
                pulse_duration=ch.get("pulse_length", 0.001024),
                sample_interval=ch.get("sample_interval", 0.000016),
                gain=ch.get("gain", 25.0),
                sa_correction=ch.get("sa_correction", 0.0),
                equivalent_beam_angle=ch.get("equivalent_beam_angle", -20.7),
                beam_width_alongship=ch.get("beamwidth_alongship", 7.0),
                beam_width_athwartship=ch.get("beamwidth_athwartship", 7.0),
                transceiver_type=ch.get("transducer_name") or "WBT",
            ))

        # Phase 5: Connect to WebSocket and buffer pings
        ws_url = base_url.replace("http://", "ws://").replace("https://", "wss://")
        ws_url += "/ws/sample-data"

        logger.info("Connecting to WebSocket: %s", ws_url)
        buffer_timeout = self.config.realtime_buffer_seconds
        batch_start = asyncio.get_event_loop().time()
        nav_poll_time = batch_start

        async with websockets.connect(ws_url, ping_interval=None, ping_timeout=None) as ws:
            logger.info("WebSocket connected to ek80-service")

            async for raw_msg in ws:
                if not self._running:
                    break

                try:
                    msg = json.loads(raw_msg)
                except (json.JSONDecodeError, TypeError):
                    continue

                msg_type = msg.get("type")

                if msg_type == "info":
                    logger.info("WS info: mode=%s, channels=%s", msg.get("mode"), msg.get("channels"))
                    continue

                if msg_type != "sample_data":
                    continue

                ch_id = msg.get("channel_id")
                if ch_id not in ch_lookup:
                    continue

                # Decode and add ping
                try:
                    timestamp = datetime.fromisoformat(msg["time"])
                    samples = np.array(msg["samples"], dtype=np.int16)
                    ch = ch_lookup[ch_id]

                    accumulator.add_ping(
                        timestamp=timestamp,
                        channel_id=ch_id,
                        power_samples=samples,
                        transmit_power=ch.get("transmit_power", 100.0),
                        pulse_duration=ch.get("pulse_length", 0.001024),
                        sample_interval=ch.get("sample_interval", 0.000016),
                        frequency=ch.get("frequency", 200000.0),
                        sound_speed=ch.get("sound_velocity", 1500.0),
                        absorption=ch.get("absorption_coefficient", 0.0),
                    )
                except Exception as e:
                    logger.debug("Failed to process ping for %s: %s", ch_id, e)
                    continue

                # Periodically poll navigation via REST
                now = asyncio.get_event_loop().time()
                if now - nav_poll_time >= 5.0:
                    nav_poll_time = now
                    try:
                        nav = await self._fetch_navigation(base_url)
                        lat = nav.get("latitude", lat) or lat
                        lon = nav.get("longitude", lon) or lon
                        heading = nav.get("heading", 0) or 0
                        accumulator.add_navigation(
                            timestamp=datetime.now(timezone.utc),
                            latitude=lat,
                            longitude=lon,
                            heading=heading,
                        )
                    except Exception:
                        pass

                # Check dual trigger: ping count OR time elapsed
                ping_count = len(accumulator)
                duration = accumulator.duration_seconds
                min_pings = self.config.realtime_min_pings
                ping_trigger = ping_count >= self.config.realtime_buffer_pings
                time_trigger = duration >= buffer_timeout

                if (ping_trigger or time_trigger) and ping_count >= min_pings:
                    trigger = "ping-count" if ping_trigger else "time"
                    logger.info(
                        "Buffer ready (%s): %d pings, %.1fs — dispatching background batch",
                        trigger, ping_count, duration,
                    )
                    try:
                        # Run CPU-intensive conversion in executor to avoid
                        # blocking the event loop (which must respond to WS
                        # keepalive pings).
                        loop = asyncio.get_running_loop()
                        echodata = await loop.run_in_executor(
                            None, accumulator.to_echodata,
                        )
                    except Exception as e:
                        logger.error("Failed to convert buffer to EchoData: %s", e, exc_info=True)
                        accumulator.clear()
                        batch_start = asyncio.get_event_loop().time()
                        continue

                    accumulator.clear()
                    batch_start = asyncio.get_event_loop().time()

                    # Dispatch processing as a background task
                    await self._dispatch_batch(echodata)

            # Flush remaining data
            if len(accumulator) >= self.config.realtime_min_pings:
                logger.info("Flushing %d remaining pings", len(accumulator))
                try:
                    echodata = accumulator.to_echodata()
                    await self._dispatch_batch(echodata)
                except Exception as e:
                    logger.error("Final batch failed: %s", e, exc_info=True)

    async def _dispatch_batch(self, echodata: Any) -> None:
        """Run ``on_batch`` in a background thread, bounded by semaphore."""
        await self._batch_sem.acquire()
        task = asyncio.create_task(self._process_batch(echodata))
        self._batch_tasks.add(task)
        task.add_done_callback(self._batch_tasks.discard)

    async def _process_batch(self, echodata: Any) -> None:
        """Execute the processing callback in a thread pool executor.

        The pipeline does heavy CPU work (xarray, Zarr I/O, matplotlib)
        that would block the event loop and prevent WebSocket keepalive
        responses.  Running it in a thread keeps the loop responsive.
        """
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                _BATCH_EXECUTOR,
                self._run_batch_sync,
                echodata,
            )
        except Exception as e:
            logger.error("Batch processing failed: %s", e, exc_info=True)
        finally:
            self._batch_sem.release()

    def _run_batch_sync(self, echodata: Any) -> None:
        """Synchronous wrapper for the async on_batch callback."""
        asyncio.run(self.on_batch(echodata, self.config))

    @staticmethod
    def _sync_get(url: str, timeout: float = 5) -> bytes:
        """Blocking HTTP GET — must be called via run_in_executor."""
        import urllib.request

        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()

    async def _async_get(self, url: str, timeout: float = 5) -> bytes:
        """Non-blocking HTTP GET using the default executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._sync_get, url, timeout)

    async def _wait_for_service(self, base_url: str, timeout: float = 300) -> None:
        """Poll the ek80-service /health endpoint until connected."""
        deadline = asyncio.get_event_loop().time() + timeout

        while asyncio.get_event_loop().time() < deadline:
            try:
                raw = await self._async_get(f"{base_url}/health", timeout=5)
                data = json.loads(raw)
                if data.get("connected"):
                    logger.info("ek80-service healthy: mode=%s", data.get("ek80_mode"))
                    return
                logger.info("ek80-service not yet connected to EK80, waiting...")
            except Exception as e:
                logger.info("ek80-service not ready (%s), retrying in 5s...", e)
            await asyncio.sleep(5)

        raise TimeoutError(f"ek80-service at {base_url} not ready after {timeout}s")

    async def _fetch_channels(self, base_url: str) -> list[dict[str, Any]]:
        """Fetch channel calibration data via GET /channels."""
        raw = await self._async_get(f"{base_url}/channels", timeout=10)
        return json.loads(raw)

    async def _fetch_navigation(self, base_url: str) -> dict[str, Any]:
        """Fetch navigation data via GET /navigation."""
        try:
            raw = await self._async_get(f"{base_url}/navigation", timeout=5)
            return json.loads(raw)
        except Exception:
            return {}
