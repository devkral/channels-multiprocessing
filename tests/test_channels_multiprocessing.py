import pytest
import async_timeout

from channels_multiprocessing import (
    MultiprocessingChannelLayer,
    BaseChannelLayer,
)


@pytest.mark.asyncio
async def test_send_receive():
    layer = MultiprocessingChannelLayer()
    message = {"type": "test.message"}
    async with async_timeout.timeout(1):
        await layer.send("test.channel", message)
    async with async_timeout.timeout(1):
        assert message == await layer.receive("test.channel")
    await layer.flush()


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
