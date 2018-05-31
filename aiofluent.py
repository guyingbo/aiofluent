import time
import socket
import struct
import msgpack
import logging
import asyncio
import async_timeout
__version__ = '0.2.4'
logger = logging.getLogger(__name__)


class EventTime(msgpack.ExtType):
    def __new__(cls, timestamp):
        seconds = int(timestamp)
        nanoseconds = int(timestamp % 1 * 10 ** 9)
        return super().__new__(
            cls,
            code=0,
            data=struct.pack(">II", seconds, nanoseconds),
        )


class FluentSender(asyncio.Protocol):
    def __init__(self,
                 tag=None,
                 host='localhost',
                 port=24224,
                 bufmax=1 * 1024 * 1024,
                 timeout=5,
                 verbose=False,
                 nanosecond_precision=False,
                 loop=None):
        self.tag = tag
        self.host = host
        self.port = port
        self.bufmax = bufmax
        self.timeout = timeout
        self.verbose = verbose
        self.nanosecond_precision = nanosecond_precision
        self.loop = loop or asyncio.get_event_loop()
        self.lock = asyncio.Lock()
        self.resume = asyncio.Event()
        self.resume.set()
        self.transport = None
        self.packer = msgpack.Packer()

    def connection_made(self, transport):
        self.transport = transport
        self.transport.set_write_buffer_limits(self.bufmax, self.bufmax)

    def connection_lost(self, exc):
        self.transport = None
        self.last_error = exc

    def pause_writing(self):
        self.resume.clear()

    def resume_writing(self):
        self.resume.set()

    async def close(self):
        async with self.lock:
            if self.transport and not self.transport.is_closing():
                self.transport.close()
            self.transport = None

    async def _reconnect(self):
        async with self.lock:
            if self.transport is None:
                if self.host is None:
                    self.server_sock, sock = socket.socketpair()
                    await self.loop.create_connection(
                        lambda: self, sock=sock
                    )
                elif self.host.startswith('unix://'):
                    await self.loop.create_unix_connection(
                        lambda: self, self.host
                    )
                else:
                    await self.loop.create_connection(
                        lambda: self, self.host, self.port
                    )

    async def _send(self, bytes_):
        try:
            async with async_timeout.timeout(self.timeout):
                while True:
                    if self.transport is None:
                        await self._reconnect()
                    await self.resume.wait()
                    if self.transport is None:
                        continue
                    self.transport.write(bytes_)
                    return True
        except socket.error as e:
            self.last_error = e
            logger.exception('socket error')
            await self.close()
            return False
        except asyncio.TimeoutError as e:
            self.last_error = e
            logger.exception('timeout error')
            return False
        except Exception as e:
            self.last_error = e
            logger.exception(str(e))
            if self.transport and self.transport.is_closing():
                self.transport = None
            return False

    def pack(self, label, data):
        if self.nanosecond_precision:
            cur_time = EventTime(time.time())
        else:
            cur_time = int(time.time())
        return self._bytes_emit_with_time(label, cur_time, data)

    async def emit(self, label, data):
        if self.nanosecond_precision:
            cur_time = EventTime(time.time())
        else:
            cur_time = int(time.time())
        return await self.emit_with_time(label, cur_time, data)

    async def emit_with_time(self, label, timestamp, data):
        bytes_ = self._bytes_emit_with_time(label, timestamp, data)
        if bytes_:
            return await self._send(bytes_)
        return False

    def _bytes_emit_with_time(self, label, timestamp, data):
        if (not self.tag) and (not label):
            raise ValueError('tag or label must be set')
        if self.tag and label:
            label = self.tag + '.' + label
        elif self.tag:
            label = self.tag
        if self.nanosecond_precision and isinstance(timestamp, float):
            timestamp = EventTime(timestamp)
        try:
            bytes_ = self._make_packet(label, timestamp, data)
        except Exception as e:
            self.last_error = e
            logger.exception('make packet error')
            return b''
            # bytes_ = self._make_packet(label, timestamp, {
            #     'level': 'CRITICAL',
            #     'message': "Can't output to log",
            #     'traceback': traceback.format_exc()})
        return bytes_

    def _make_packet(self, label, timestamp, data):
        packet = (label, timestamp, data)
        if self.verbose:
            print(packet)
        return self.packer.pack(packet)
