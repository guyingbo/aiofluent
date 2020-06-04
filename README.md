# aiofluent

![Python package](https://github.com/guyingbo/aiofluent/workflows/Python%20package/badge.svg)
[![PyPI](https://img.shields.io/pypi/pyversions/aiofluent-python.svg)](https://pypi.python.org/pypi/aiofluent-python)
[![PyPI](https://img.shields.io/pypi/v/aiofluent-python.svg)](https://pypi.python.org/pypi/aiofluent-python)
[![PyPI](https://img.shields.io/pypi/format/aiofluent-python.svg)](https://pypi.python.org/pypi/aiofluent-python)
[![PyPI](https://img.shields.io/pypi/l/aiofluent-python.svg)](https://pypi.python.org/pypi/aiofluent-python)
[![codecov](https://codecov.io/gh/guyingbo/aiofluent/branch/master/graph/badge.svg)](https://codecov.io/gh/guyingbo/aiofluent)


An asynchronous fluentd client libary. Inspired by [fluent-logger-python](https://github.com/fluent/fluent-logger-python)

## Requirements

- Python 3.5 or greater
- msgpack
- async-timeout

## Installation

~~~
pip install aiofluent-python
~~~

## Example

~~~python
import asyncio
from aiofluent import FluentSender
sender = FluentSender()


async def go():
    await sender.emit('tag', {'name': 'aiofluent'})
    await sender.close()


asyncio.run(go())
~~~
