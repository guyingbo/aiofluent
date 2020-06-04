import pytest
from aiofluent.glogging import GLoggingFluentSender, HttpRequest, LogSeverity


@pytest.mark.asyncio
async def test_glogging():
    sender = GLoggingFluentSender()
    await sender.log("tag", "this is a message", LogSeverity.INFO, a=3)
    try:
        raise NameError
    except Exception:
        await sender.report_error(http_request=HttpRequest(requestMethod="GET"))
    await sender.close()
