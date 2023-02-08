import asyncio

import async_timeout
import pytest
from contextlib import AsyncExitStack

from channels.exceptions import ChannelFull
from channels_multiprocessing import MultiprocessingChannelLayer


@pytest.mark.asyncio
async def test_send_receive():
    """
    Makes sure we can send a message to a normal channel then receive it.
    """

    async with AsyncExitStack() as stack:
        layer = MultiprocessingChannelLayer(capacity=3)
        stack.push_async_callback(layer.close)

        await layer.send(
            "test-channel-1", {"type": "test.message", "text": "Ahoy-hoy!"}
        )
        message = await layer.receive("test-channel-1")
        assert message["type"] == "test.message"
        assert message["text"] == "Ahoy-hoy!"
        await stack.aclose()


@pytest.mark.asyncio
async def test_send_capacity():
    """
    Makes sure we get ChannelFull when we hit the send capacity
    """
    async with AsyncExitStack() as stack:
        layer = MultiprocessingChannelLayer(capacity=3)
        stack.push_async_callback(layer.close)
        await asyncio.gather(
            layer.send("test-channel-1", {"type": "test.message"}),
            layer.send("test-channel-1", {"type": "test.message"}),
            layer.send("test-channel-1", {"type": "test.message"}),
        )
        with pytest.raises(ChannelFull):
            await layer.send("test-channel-1", {"type": "test.message"})
        await stack.aclose()


@pytest.mark.asyncio
async def test_process_local_send_receive():
    """
    Makes sure we can send a message to a process-local channel then receive
    it.
    """

    async with AsyncExitStack() as stack:
        layer = MultiprocessingChannelLayer(capacity=3)
        stack.push_async_callback(layer.close)
        channel_name = await layer.new_channel()
        await layer.send(
            channel_name, {"type": "test.message", "text": "Local only please"}
        )
        message = await layer.receive(channel_name)
        assert message["type"] == "test.message"
        assert message["text"] == "Local only please"
        await stack.aclose()


@pytest.mark.asyncio
async def test_multi_send_receive():
    """
    Tests overlapping sends and receives, and ordering.
    """
    async with AsyncExitStack() as stack:
        layer = MultiprocessingChannelLayer()
        stack.push_async_callback(layer.close)
        await layer.send("test-channel-3", {"type": "message.1"})
        await layer.send("test-channel-3", {"type": "message.2"})
        await layer.send("test-channel-3", {"type": "message.3"})
        assert (await layer.receive("test-channel-3"))["type"] == "message.1"
        assert (await layer.receive("test-channel-3"))["type"] == "message.2"
        assert (await layer.receive("test-channel-3"))["type"] == "message.3"
        await stack.aclose()


@pytest.mark.asyncio
async def test_groups_basic():
    """
    Tests basic group operation.
    """
    async with AsyncExitStack() as stack:
        layer = MultiprocessingChannelLayer()
        stack.push_async_callback(layer.close)
        await layer.group_add("test-group", "test-gr-chan-1")
        await layer.group_add("test-group", "test-gr-chan-2")
        await layer.group_add("test-group", "test-gr-chan-3")
        await layer.group_discard("test-group", "test-gr-chan-2")
        await layer.group_send("test-group", {"type": "message.1"})
        # Make sure we get the message on the two channels that were in
        async with async_timeout.timeout(1):
            assert (await layer.receive("test-gr-chan-1"))[
                "type"
            ] == "message.1"
            assert (await layer.receive("test-gr-chan-3"))[
                "type"
            ] == "message.1"
        # Make sure the removed channel did not get the message
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(1):
                await layer.receive("test-gr-chan-2")


@pytest.mark.asyncio
async def test_groups_parallel():
    """
    Tests basic group operation.
    """
    async with AsyncExitStack() as stack:
        layer = MultiprocessingChannelLayer()
        stack.push_async_callback(layer.close)
        await asyncio.gather(
            layer.group_add("test-group", "test-gr-chan-1"),
            layer.group_add("test-group", "test-gr-chan-2"),
            layer.group_add("test-group", "test-gr-chan-3"),
        )
        await layer.group_discard("test-group", "test-gr-chan-2")
        await layer.group_send("test-group", {"type": "message.1"})
        # Make sure we get the message on the two channels that were in
        async with async_timeout.timeout(1):
            assert (await layer.receive("test-gr-chan-1"))[
                "type"
            ] == "message.1"
            assert (await layer.receive("test-gr-chan-3"))[
                "type"
            ] == "message.1"
        # Make sure the removed channel did not get the message
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(1):
                await layer.receive("test-gr-chan-2")


@pytest.mark.asyncio
async def test_groups_channel_full():
    """
    Tests that group_send ignores ChannelFull
    """
    async with AsyncExitStack() as stack:
        layer = MultiprocessingChannelLayer(capacity=3)
        stack.push_async_callback(layer.close)
        await layer.group_add("test-group", "test-gr-chan-1")
        await layer.group_send("test-group", {"type": "message.1"})
        await layer.group_send("test-group", {"type": "message.1"})
        await layer.group_send("test-group", {"type": "message.1"})
        await layer.group_send("test-group", {"type": "message.1"})
        await layer.group_send("test-group", {"type": "message.1"})


@pytest.mark.asyncio
async def test_groups_channel_full_parallel():
    """
    Tests that group_send ignores ChannelFull
    """
    async with AsyncExitStack() as stack:
        layer = MultiprocessingChannelLayer(capacity=3)
        stack.push_async_callback(layer.close)
        await layer.group_add("test-group", "test-gr-chan-1")
        await asyncio.gather(
            *[
                layer.group_send("test-group", {"type": "message.1"})
                for i in range(500)
            ]
        )


@pytest.mark.asyncio
async def test_expiry_single():
    """
    Tests that a message can expire.
    """
    async with AsyncExitStack() as stack:
        layer = MultiprocessingChannelLayer(expiry=0.1)
        stack.push_async_callback(layer.close)
        await layer.send("test-channel-1", {"type": "message.1"})
        assert len(layer.channels) == 1

        await asyncio.sleep(0.1)

        # Message should have expired and been dropped.
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(0.5):
                await layer.receive("test-channel-1")

        # Channel should be cleaned up.
        assert len(layer.channels) == 0


@pytest.mark.asyncio
async def test_expiry_unread():
    """
    Tests that a message on a channel can expire and be cleaned up even if
    the channel is not read from again.
    """

    async with AsyncExitStack() as stack:
        layer = MultiprocessingChannelLayer(expiry=0.1)
        stack.push_async_callback(layer.close)
        await layer.send("test-channel-1", {"type": "message.1"})

        await asyncio.sleep(0.1)

        await layer.send("test-channel-2", {"type": "message.2"})
        assert len(layer.channels) == 2
        assert (await layer.receive("test-channel-2"))["type"] == "message.2"
        # Both channels should be cleaned up.
        assert len(layer.channels) == 0


@pytest.mark.asyncio
async def test_expiry_multi():
    """
    Tests that multiple messages can expire.
    """
    async with AsyncExitStack() as stack:
        layer = MultiprocessingChannelLayer(expiry=0.1)
        stack.push_async_callback(layer.close)
        assert len(layer.channels) == 0
        await layer.send("test-channel-1", {"type": "message.1"})
        await layer.send("test-channel-1", {"type": "message.2"})
        await layer.send("test-channel-1", {"type": "message.3"})
        assert (await layer.receive("test-channel-1"))["type"] == "message.1"

        await asyncio.sleep(0.1)
        await layer.send("test-channel-1", {"type": "message.4"})
        assert (await layer.receive("test-channel-1"))["type"] == "message.4"

        # The second and third message should have expired and been dropped.
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(0.5):
                await layer.receive("test-channel-1")

        # Channel should be cleaned up.
        assert len(layer.channels) == 0
