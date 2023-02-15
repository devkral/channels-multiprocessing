import pytest
import async_timeout
import asyncio
import random
from contextlib import asynccontextmanager

from channels_multiprocessing import (
    MultiprocessingChannelLayer,
    BaseChannelLayer,
)


@asynccontextmanager
async def cleanup(layer):
    try:
        yield layer
    finally:
        await layer.flush()
        await layer.close()


@pytest.mark.asyncio
async def test_send_receive_simple():
    layer = MultiprocessingChannelLayer()
    message = {"type": "test.message"}
    async with async_timeout.timeout(1):
        await layer.send("test.channel", message)
    async with async_timeout.timeout(1):
        assert message == await layer.receive("test.channel")
    await layer.flush()
    await layer.close()


async def timeout_send(layer, channel, message):
    await asyncio.sleep(random.random() / 2)
    async with async_timeout.timeout(1):
        await layer.send(channel, message)


async def timeout_receive(layer, channel, message):
    await asyncio.sleep(random.random() / 2)
    async with async_timeout.timeout(10):
        assert message == await layer.receive(channel)


def generate_messages(layer, channels=2):
    message = {"type": "test.message"}
    for c in range(channels):
        channel = f"test.channel{c}"
        for i in range(30):
            yield asyncio.ensure_future(
                timeout_send(layer, channel, message)
                if i % 2 == 0
                else timeout_receive(layer, channel, message)
            )
    for c in range(channels):
        channel = f"test.channel{c}"
        for i in range(30):
            yield asyncio.ensure_future(
                timeout_send(layer, channel, message)
                if i % 2 == 0
                else timeout_receive(layer, channel, message)
            )


@pytest.mark.asyncio
async def test_send_receive_massive_parallel():
    async with cleanup(MultiprocessingChannelLayer(capacity=60)) as layer:
        for result in asyncio.as_completed(list(generate_messages(layer, 1))):
            await result


@pytest.mark.asyncio
async def test_send_receive_massive_multichannel_parallel():
    async with cleanup(MultiprocessingChannelLayer(capacity=60)) as layer:
        for result in asyncio.as_completed(list(generate_messages(layer, 10))):
            await result


@pytest.mark.asyncio
async def test_send_receive_massive_timeout_parallel():
    async with cleanup(
        MultiprocessingChannelLayer(capacity=30, expiry=0.1)
    ) as layer:
        for result in asyncio.as_completed(list(generate_messages(layer))):
            try:
                await result
            except Exception:
                pass


@pytest.mark.parametrize(
    "method",
    [
        BaseChannelLayer().valid_channel_name,
        BaseChannelLayer().valid_group_name,
    ],
)
@pytest.mark.parametrize(
    "channel_name,expected_valid",
    [("¯\\_(ツ)_/¯", False), ("chat", True), ("chat" * 100, False)],
)
def test_channel_and_group_name_validation(
    method, channel_name, expected_valid
):
    if expected_valid:
        method(channel_name)
    else:
        with pytest.raises(TypeError):
            method(channel_name)
