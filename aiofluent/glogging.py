"""Google Cloud Platform Logging Utility"""
import enum
import traceback
from typing import Any, Dict, Optional

from . import FluentSender


class HttpRequest:
    def __init__(
        self,
        *,
        requestMethod: Optional[str] = None,
        requestUrl: Optional[str] = None,
        requestSize: Optional[str] = None,
        status: Optional[int] = None,
        responseSize: Optional[str] = None,
        userAgent: Optional[str] = None,
        remoteIp: Optional[str] = None,
        serverIp: Optional[str] = None,
        referer: Optional[str] = None,
        latency: Optional[str] = None,
        cacheLookup: Optional[bool] = None,
        cacheHit: Optional[bool] = None,
        cacheValidatedWithOriginServer: Optional[bool] = None,
        cacheFillBytes: Optional[str] = None,
        protocol: Optional[str] = None
    ):
        for k, v in locals().items():
            if k != "self" and v is not None:
                self.__dict__[k] = v

    def as_dict(self):
        return self.__dict__


class LogSeverity(enum.IntEnum):
    DEFAULT = 0
    DEBUG = 100
    INFO = 200
    NOTICE = 300
    WARNING = 400
    ERROR = 500
    CRITICAL = 600
    ALERT = 700
    EMERGENCY = 800


class LogEntryField(enum.Enum):
    trace = "logging.googleapis.com/trace"
    spanId = "logging.googleapis.com/spanId"
    operation = "logging.googleapis.com/operation"
    sourceLocation = "logging.googleapis.com/sourceLocation"


class GLoggingFluentSender(FluentSender):
    async def log(
        self,
        tag: str,
        message: str,
        severity: Optional[LogSeverity] = None,
        http_request: Optional[HttpRequest] = None,
        **kwargs
    ):
        data = {}  # type: Dict[str, Any]
        data["message"] = message
        if severity is not None:
            data["severity"] = severity.value
        if http_request:
            data["httpRequest"] = http_request.as_dict()
        if kwargs:
            for field in list(LogEntryField):
                try:
                    value = kwargs.pop(field.name)
                except KeyError:
                    continue
                data[field.value] = value
            data.update(kwargs)
        await self.emit(tag, data)

    async def report_error(
        self, severity: Optional[LogSeverity] = LogSeverity.ERROR, **kwargs
    ):
        message = traceback.format_exc()
        await self.log("errors", message, severity, **kwargs)
