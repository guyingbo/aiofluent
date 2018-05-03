# aiofluent

[![Build Status](https://travis-ci.org/guyingbo/aiofluent.svg?branch=master)](https://travis-ci.org/guyingbo/aiofluent)
[![PyPI](https://img.shields.io/pypi/pyversions/aiofluent-python.svg)](https://pypi.python.org/pypi/aiofluent-python)
[![PyPI](https://img.shields.io/pypi/v/aiofluent-python.svg)](https://pypi.python.org/pypi/aiofluent-python)
[![PyPI](https://img.shields.io/pypi/format/aiofluent-python.svg)](https://pypi.python.org/pypi/aiofluent-python)
[![PyPI](https://img.shields.io/pypi/l/aiofluent-python.svg)](https://pypi.python.org/pypi/aiofluent-python)


A fluentd client libary intended to work with asyncio. Inspires by [fluent-logger-python](https://github.com/fluent/fluent-logger-python)

## Requirements

- Python 3.5 or greater
- msgpack-python
- async-timeout

## Installation

~~~
pip install aiofluent-python
~~~

## Example

~~~python
import asyncio
from aiofluent import FluentSender
loop = asyncio.get_event_loop()
sender = FluentSender('tag')


async def go():
    await sender.emit('label', {'name': 'aiofluent'})
    await sender.close()


loop.run_until_complete(go())
loop.run_until_complete(loop.shutdown_asyncgens())
loop.close()
~~~
