import time
import socket
import pytest
import asyncio
import msgpack
from aiofluent import FluentSender

dic = {"name": "test"}


async def t1():
    server_sock, sock = socket.socketpair()
    sender = FluentSender("tag", host=sock, bufmax=10, timeout=1)
    await sender.emit("label", dic)
    server_sock.send(b"haha")
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

    for i in range(40):
        r = await sender.emit("large", "long" * 100)
        if not r:
            server_sock.recv(1024)

    await sender.close()


async def t2():
    sender = FluentSender("tag", nanosecond_precision=True)
    await sender.emit("label", dic)
    await sender.close()
    await sender.emit_with_time("label2", time.time(), dic)
    sender.pack("label3", dic)
    await sender._send("string")
    await sender.close()


async def t3():
    sender = FluentSender("tag", host="unix:///tmp/a.sock")
    await sender.emit("label", dic)
    await sender.close()
    await sender.emit("label2", dic)
    await sender.close()
    sender.pack("label3", dic)
    sender.tag = None
    with pytest.raises(ValueError):
        await sender.emit(None, "nothing")


def test_fluent():
    loop = asyncio.get_event_loop()

    async def go():
        await t1()
        await t2()
        await t3()

    loop.run_until_complete(go())
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
