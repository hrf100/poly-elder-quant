import time
import psutil
from backend.storage.db import Database


class SystemMonitor:
    """Tracks real-time CPU, RAM, Swap, Disk, and real-time + persistent cumulative network traffic."""

    def __init__(self, db: Database):
        self.db = db
        self.start_time = time.time()
        net = psutil.net_io_counters()
        self._last_ts = time.time()
        self._last_rx = net.bytes_recv
        self._last_tx = net.bytes_sent
        self._last_persist = time.time()

        saved = self.db.get_json("net_totals", {"rx_bytes": 0, "tx_bytes": 0})
        self.cum_rx = int(saved.get("rx_bytes", 0))
        self.cum_tx = int(saved.get("tx_bytes", 0))
        psutil.cpu_percent(interval=None)

    def sample(self) -> dict:
        now = time.time()
        dt = max(0.2, now - self._last_ts)
        net = psutil.net_io_counters()

        d_rx = max(0, net.bytes_recv - self._last_rx)
        d_tx = max(0, net.bytes_sent - self._last_tx)
        self.cum_rx += d_rx
        self.cum_tx += d_tx

        rx_kbps = round(d_rx / dt / 1024.0, 2)
        tx_kbps = round(d_tx / dt / 1024.0, 2)

        self._last_ts = now
        self._last_rx = net.bytes_recv
        self._last_tx = net.bytes_sent

        if now - self._last_persist >= 30:
            self.db.set_json("net_totals", {
                "rx_bytes": self.cum_rx,
                "tx_bytes": self.cum_tx,
                "last_os_rx": net.bytes_recv,
                "last_os_tx": net.bytes_sent,
            })
            self._last_persist = now

        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        disk = psutil.disk_usage("/")
        proc = psutil.Process()
        proc_rss_mb = round(proc.memory_info().rss / (1024 * 1024), 1)

        return {
            "uptime_sec": int(now - self.start_time),
            "cpu_percent": round(psutil.cpu_percent(interval=None), 1),
            "cpu_cores": psutil.cpu_count(logical=True) or 1,
            "mem_percent": round(mem.percent, 1),
            "mem_used_mb": round(mem.used / (1024 * 1024), 1),
            "mem_total_mb": round(mem.total / (1024 * 1024), 1),
            "proc_rss_mb": proc_rss_mb,
            "swap_percent": round(swap.percent, 1),
            "swap_used_mb": round(swap.used / (1024 * 1024), 1),
            "swap_total_mb": round(swap.total / (1024 * 1024), 1),
            "disk_percent": round(disk.percent, 1),
            "disk_free_gb": round(disk.free / (1024 ** 3), 2),
            "net_rx_kbps": rx_kbps,
            "net_tx_kbps": tx_kbps,
            "net_cum_rx_mb": round(self.cum_rx / (1024 * 1024), 2),
            "net_cum_tx_mb": round(self.cum_tx / (1024 * 1024), 2),
        }
