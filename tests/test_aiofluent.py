import asyncio
from aiofluent import FluentSender
loop = asyncio.get_event_loop()


def test_fluent():
    sender = FluentSender()
    loop.run_until_complete(sender.close())
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
