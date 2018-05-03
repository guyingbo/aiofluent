import time
import struct
import socket
import traceback
import asyncio
import msgpack
__version__ = '0.1.0'


class EventTime(msgpack.ExtType):
    def __new__(cls, timestamp):
        seconds = int(timestamp)
        nanoseconds = int(timestamp % 1 * 10 ** 9)
        return super().__new__(
            cls,
            code=0,
            data=struct.pack(">II", seconds, nanoseconds),
        )


class FluentSender:
    def __init__(self,
                 tag=None,
                 host='localhost',
                 port=24224,
                 bufmax=1 * 1024 * 1024,
                 timeout=3.0,
                 verbose=False,
                 buffer_overflow_handler=None,
                 nanosecond_precision=False,
                 loop=None):
        self.tag = tag
        self.sock = None
        self.pendings = None
        self.host = host
        self.port = port
        self.bufmax = bufmax
        self.timeout = timeout
        self.verbose = verbose
        self.buffer_overflow_handler = buffer_overflow_handler
        self.nanosecond_precision = nanosecond_precision
        self.loop = loop or asyncio.get_event_loop()
        self.lock = asyncio.Lock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, typ, value, tb):
        await self.close()

    def emit_later(self, label, data):
        return asyncio.ensure_future(self.emit(label, data))

    async def emit(self, label, data):
        if self.nanosecond_precision:
            cur_time = EventTime(time.time())
        else:
            cur_time = int(time.time())
        return await self.emit_with_time(label, cur_time, data)

    async def emit_with_time(self, label, timestamp, data):
        bytes_ = self._bytes_emit_with_time(label, timestamp, data)
        return await self._send(bytes_)

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
            bytes_ = self._make_packet(label, timestamp, {
                'level': 'CRITICAL',
                'message': "Can't output to log",
                'traceback': traceback.format_exc()})
        return bytes_

    async def close(self):
        async with self.lock:
            if self.pendings:
                try:
                    await self._send_data(self.pendings)
                except Exception:
                    self._call_buffer_overflow_handler(self.pendings)
            self._close()
            self.pendings = None

    async def _reconnect(self):
        if not self.sock:
            if self.host.startswith('unix://'):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.setblocking(False)
                await asyncio.wait_for(
                    self.loop.sock_connect(sock, self.host[len('unix://'):]),
                    self.timeout
                )
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setblocking(False)
                await asyncio.wait_for(
                    self.loop.sock_connect(sock, (self.host, self.port)),
                    self.timeout
                )
            self.sock = sock

    def _close(self):
        if self.sock:
            self.sock.close()
        self.sock = None

    def _make_packet(self, label, timestamp, data):
        packet = (label, timestamp, data)
        if self.verbose:
            print(packet)
        return msgpack.packb(packet)

    async def _send(self, bytes_):
        async with self.lock:
            return await self._send_internal(bytes_)

    async def _send_internal(self, bytes_):
        # buffering
        if self.pendings:
            self.pendings += bytes_
            bytes_ = self.pendings
        try:
            await self._send_data(bytes_)
            # send finished
            self.pendings = None
            return True
        except (socket.error, asyncio.TimeoutError) as e:
            self.last_error = e
            self._close()
            # clear buffer if it exceeds max bufer size
            if self.pendings and len(self.pendings) > self.bufmax:
                self._call_buffer_overflow_handler(self.pendings)
                self.pendings = None
            else:
                self.pendings = bytes_
            return False

    async def _send_data(self, bytes_):
        await self._reconnect()
        await asyncio.wait_for(
            self.loop.sock_sendall(self.sock, bytes_),
            self.timeout
        )

    def _call_buffer_overflow_handler(self, pendings):
        try:
            if self.buffer_overflow_handler:
                self.buffer_overflow_handler(pendings)
        except Exception as e:
            # User should care any exception in handler
            pass
