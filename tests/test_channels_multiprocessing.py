import pytest
import async_timeout
import asyncio
import random

from channels_multiprocessing import (
    MultiprocessingChannelLayer,
    BaseChannelLayer,
)


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


async def timeout_send(layer, message):
    await asyncio.sleep(random.random() / 2)
    async with async_timeout.timeout(1):
        await layer.send("test.channel", message)


async def timeout_receive(layer, message):
    await asyncio.sleep(random.random() / 2)
    async with async_timeout.timeout(10):
        assert message == await layer.receive("test.channel")


@pytest.mark.asyncio
async def test_send_receive_wild_parallel():
    layer = MultiprocessingChannelLayer()
    message = {"type": "test.message"}
    await asyncio.wait(
        [
            asyncio.ensure_future(
                timeout_send(layer, message)
                if i % 2 == 0
                else timeout_receive(layer, message)
            )
            for i in range(120)
        ]
    )
    await layer.flush()
    await layer.close()


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
