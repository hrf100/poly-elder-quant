import time
from collections import deque
from email.utils import parsedate_to_datetime
from typing import Optional


class ServerClock:
    """
    Calibrated Server Clock (v37):
    - Estimates `offset_ms = local_ms - server_ms` using the minimum per-second sample
      over a sliding 30-second window (separating constant clock skew from positive network jitter).
    - Clamps offset against HTTP `Date` / RTT response anchors (+1000ms slack) so network
      congestion spikes never bias the clock early.
    - Automatically resets calibration on OS clock jump (NTP step / VM resume).
    """

    WINDOW_SEC = 30
    ANCHOR_SLACK_MS = 1000.0

    def __init__(self):
        self._buckets: deque[tuple[int, float]] = deque(maxlen=40)
        self.offset_ms: float = 0.0
        self.anchor_offset_ms: Optional[float] = None
        self.anchor_updated_at: float = 0.0
        self.samples_seen: int = 0
        self._last_wall_mono_diff: float = time.time() - time.monotonic()

    def _check_clock_jump(self):
        diff = time.time() - time.monotonic()
        if abs(diff - self._last_wall_mono_diff) > 2.5:
            self._buckets.clear()
            self.anchor_offset_ms = None
            self.offset_ms = 0.0
        self._last_wall_mono_diff = diff

    def observe(self, server_ts_ms: float) -> Optional[float]:
        """Observe a server timestamp (in ms) from WS / REST messages and return estimated network latency (ms)."""
        if not server_ts_ms or server_ts_ms <= 1_500_000_000_000:
            return None
        self._check_clock_jump()
        local_ms = time.time() * 1000.0
        sample = local_ms - float(server_ts_ms)
        if abs(sample) > 600_000.0:
            return None

        sec = int(local_ms // 1000)
        if self._buckets and self._buckets[-1][0] == sec:
            if sample < self._buckets[-1][1]:
                self._buckets[-1] = (sec, sample)
        else:
            self._buckets.append((sec, sample))

        cutoff = sec - self.WINDOW_SEC
        while self._buckets and self._buckets[0][0] < cutoff:
            self._buckets.popleft()

        min_off = min(v for _, v in self._buckets)
        if self.anchor_offset_ms is not None and (time.time() - self.anchor_updated_at) <= 180.0:
            min_off = min(min_off, self.anchor_offset_ms + self.ANCHOR_SLACK_MS)

        self.offset_ms = round(min_off, 1)
        self.samples_seen += 1
        return max(0.0, round(local_ms - self.offset_ms - float(server_ts_ms), 1))

    def observe_http_date(self, date_header: Optional[str], rtt_ms: float = 0.0) -> None:
        if not date_header:
            return
        try:
            dt = parsedate_to_datetime(date_header)
            srv_ms = dt.timestamp() * 1000.0 + 500.0
            local_ms = time.time() * 1000.0 - max(0.0, rtt_ms) * 0.5
            est = local_ms - srv_ms
            if abs(est) <= 600_000.0:
                self._check_clock_jump()
                self.anchor_offset_ms = round(est, 1)
                self.anchor_updated_at = time.time()
                if not self._buckets:
                    self.offset_ms = self.anchor_offset_ms
                else:
                    self.offset_ms = round(min(self.offset_ms, self.anchor_offset_ms + self.ANCHOR_SLACK_MS), 1)
        except Exception:
            pass

    def observe_rtt_server_ms(self, server_ms: float, rtt_ms: float) -> None:
        if not server_ms or server_ms <= 1_500_000_000_000:
            return
        self._check_clock_jump()
        local_mid_ms = time.time() * 1000.0 - max(0.0, rtt_ms) * 0.5
        est = local_mid_ms - float(server_ms)
        if abs(est) <= 600_000.0:
            self.anchor_offset_ms = round(est, 1)
            self.anchor_updated_at = time.time()
            if not self._buckets:
                self.offset_ms = self.anchor_offset_ms
            else:
                self.offset_ms = round(min(self.offset_ms, self.anchor_offset_ms + self.ANCHOR_SLACK_MS), 1)

    def server_ms(self) -> float:
        return time.time() * 1000.0 - self.offset_ms

    def server_now(self) -> float:
        return self.server_ms() / 1000.0

    def drift_ms(self) -> float:
        return round(self.offset_ms, 1)

    def latency_ms(self, server_ts_ms: float) -> Optional[float]:
        if not server_ts_ms or server_ts_ms <= 1_500_000_000_000:
            return None
        return max(0.0, round(self.server_ms() - float(server_ts_ms), 1))
