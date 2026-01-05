# snowflake_u64.py
import time
import asyncio
from bson.int64 import Int64

class SnowflakeU64:
    """
    64-bit layout (big-endian):
        41 bits  = milliseconds since custom EPOCH
        10 bits  = node_id   (0-1023)
        12 bits  = per-ms sequence (0-4095)
    """
    EPOCH_MS = 1735689600000  # 2025-01-01T00:00:00Z

    def __init__(self, node_id: int):
        if not 0 <= node_id <= 1023:
            raise ValueError("node_id must be 0-1023")
        self.node_id   = node_id
        self._last_ms  = -1
        self._seq      = 0
        self._lock     = asyncio.Lock()  # safe for async code

    async def __call__(self) -> Int64:
        async with self._lock:
            now_ms = int(time.time() * 1000)
            if now_ms == self._last_ms:
                self._seq = (self._seq + 1) & 0xFFF  # 12 bits
                if self._seq == 0:                   # sequence rollover
                    while int(time.time() * 1000) == now_ms:
                        await asyncio.sleep(0)       # wait next millisecond
                    now_ms = int(time.time() * 1000)
            else:
                self._seq = 0
            self._last_ms = now_ms

            elapsed = now_ms - self.EPOCH_MS
            if elapsed < 0 or elapsed >= (1 << 41):
                raise OverflowError("timestamp outside allowed range")

            id64 = (elapsed << 22) | (self.node_id << 12) | self._seq
            return Int64(id64)
