import asyncio
import logging
import socket
import struct
import time
from typing import Any, Callable, Optional, Union

import async_timeout
import msgpack  # type: ignore

__version__ = "0.3.0"
logger = logging.getLogger(__name__)


def _nano_time(timestamp: Union[int, float]) -> msgpack.ExtType:
    seconds = int(timestamp)
    nanoseconds = int((timestamp % 1) * (10 ** 9))
    return msgpack.ExtType(code=0, data=struct.pack(">II", seconds, nanoseconds))


class FluentSender(asyncio.Protocol):
    def __init__(
        self,
        host: Union[str, socket.socket] = "localhost",
        port: int = 24224,
        bufmax: int = 256 * 1024,
        timeout: int = 5,
        nanosecond_precision: bool = False,
        error_callback: Optional[
            Callable[[Exception], Any]
        ] = lambda e: logger.exception(str(e)),
    ):
        self.host = host
        self.port = port
        self.bufmax = bufmax
        self.timeout = timeout
        self.nanosecond_precision = nanosecond_precision
        self.lock = asyncio.Lock()
        self.resume = asyncio.Event()
        self.resume.set()
        self.transport = None  # type: Optional[asyncio.Transport]
        self.packer = msgpack.Packer()
        self.error_callback = error_callback

    def connection_made(self, transport) -> None:
        self.transport = transport
        transport.set_write_buffer_limits(self.bufmax, min(16384, self.bufmax))
        self.resume.set()

    def connection_lost(self, exc: Optional[Exception]):
        if self.transport:
            self.transport.close()
        self.transport = None
        if self.error_callback and exc:
            self.error_callback(exc)

    def pause_writing(self):
        self.resume.clear()

    def resume_writing(self):
        self.resume.set()

    async def close(self):
        async with self.lock:
            if self.transport and not self.transport.is_closing():
                try:
                    async with async_timeout.timeout(self.timeout):
                        while self.transport:
                            size = self.transport.get_write_buffer_size()
                            if size == 0:
                                break
                            await asyncio.sleep(0.001)
                except asyncio.TimeoutError as e:
                    if self.error_callback:
                        self.error_callback(e)
                if self.transport:
                    self.transport.close()
            self.transport = None

    async def _reconnect(self):
        async with self.lock:
            if self.transport is None or self.transport.is_closing():
                loop = asyncio.get_event_loop()
                if isinstance(self.host, socket.socket):
                    await loop.create_connection(lambda: self, sock=self.host)
                elif self.host.startswith("unix://"):
                    await loop.create_unix_connection(lambda: self, self.host)
                else:
                    await loop.create_connection(lambda: self, self.host, self.port)

    async def _send(self, bytes_: bytes) -> bool:
        try:
            async with async_timeout.timeout(self.timeout):
                if self.transport is None:
                    await self._reconnect()
                assert self.transport is not None, "connection lost"
                await self.resume.wait()
                self.transport.write(bytes_)
                return True
        except Exception as e:
            if self.error_callback:
                self.error_callback(e)
            return False

    def pack(self, tag: str, data: Any) -> bytes:
        return self._bytes_emit_with_time(tag, time.time(), data)

    async def emit(self, tag: str, data: Any) -> bool:
        return await self.emit_with_time(tag, time.time(), data)

    async def emit_with_time(
        self, tag: str, timestamp: Union[int, float], data: Any
    ) -> bool:
        bytes_ = self._bytes_emit_with_time(tag, timestamp, data)
        if bytes_:
            return await self._send(bytes_)
        return False

    def _bytes_emit_with_time(
        self, tag: str, timestamp: Union[int, float], data: Any
    ) -> bytes:
        if not tag:
            raise ValueError("tag must be set")
        if self.nanosecond_precision:
            timestamp = _nano_time(timestamp)
        else:
            timestamp = int(timestamp)
        return self.packer.pack((tag, timestamp, data))
