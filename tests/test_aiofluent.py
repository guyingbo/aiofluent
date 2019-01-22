import socket
import asyncio
import msgpack
from aiofluent import FluentSender


def test_fluent():
    loop = asyncio.get_event_loop()
    dic = {"name": "test"}

    async def go():
        server_sock, sock = socket.socketpair()
        sender = FluentSender("tag", host=sock)
        await sender.emit("label", dic)
        server_sock.send(b"haha")
        data = server_sock.recv(1024)
        label, timestamp, obj = msgpack.unpackb(data, encoding="utf-8")
        assert label == "tag.label"
        assert obj == dic

        await asyncio.sleep(0.1)
        await sender.emit("label2", dic)
        data = server_sock.recv(1024)
        label, timestamp, obj = msgpack.unpackb(data, encoding="utf-8")
        assert label == "tag.label2"
        assert obj == dic

        await sender.close()

        sender = FluentSender("tag")
        await sender.emit("label", dic)
        await sender.close()
        await sender.emit("label2", dic)
        await sender.close()

        sender = FluentSender("tag", port=24225)
        await sender.emit("label", dic)
        await sender.close()
        await sender.emit("label2", dic)
        await sender.close()

    loop.run_until_complete(go())
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
