import time
import socket
import pytest
import msgpack
import asyncio
from aiofluent import FluentSender

dic = {"name": "test"}


@pytest.mark.asyncio
async def test1():
    server_sock, sock = socket.socketpair()
    sender = FluentSender("tag", host=sock, bufmax=10, timeout=1)
    await sender.emit("label", dic)
    data = server_sock.recv(1024)
    label, timestamp, obj = msgpack.unpackb(data, encoding="utf-8")
    assert label == "tag.label"
    assert obj == dic
    assert sender.last_error is None
    await sender.emit(None, object())
    assert str(sender.last_error) is not None

    await sender.emit("label2", dic)
    data = server_sock.recv(1024)
    label, timestamp, obj = msgpack.unpackb(data, encoding="utf-8")
    assert label == "tag.label2"
    assert obj == dic

    for i in range(30):
        r = await sender.emit("large", "long" * 200)
        if not r:
            server_sock.recv(256)

    await sender.close()


async def send(sender):
    for i in range(2000):
        await sender.emit("label", dic)
    await sender.close()


@pytest.mark.asyncio
async def test2(event_loop):
    sender = FluentSender("tag", nanosecond_precision=True, bufmax=10, timeout=0.01)
    await sender.emit("label", dic)
    await sender.close()
    await sender.emit_with_time("label2", time.time(), dic)
    sender.pack("label3", dic)
    await sender.close()
    tasks = []
    for i in range(10):
        task = event_loop.create_task(send(sender))
        tasks.append(task)
    await asyncio.gather(*tasks)
    await sender.emit("label", "hello" * 100000)
    await sender.close()


@pytest.mark.asyncio
async def test3():
    sender = FluentSender("tag", host="unix:///tmp/a.sock")
    r = await sender.emit("label", dic)
    assert not r
    await sender.close()
    await sender.emit("label2", dic)
    await sender.close()
    sender.pack("label3", dic)
    sender.tag = None
    with pytest.raises(ValueError):
        await sender.emit(None, "nothing")
