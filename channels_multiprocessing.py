from copy import deepcopy
import multiprocessing
from channels.layers import BaseChannelLayer
from channels.exceptions import ChannelFull
from multiprocessing.managers import SyncManager
from multiprocessing import get_context
import time
import random
import string
from queue import Queue, Full, Empty
import atexit
from concurrent.futures import ThreadPoolExecutor
import asyncio


@atexit.register
def kill_children():
    [p.kill() for p in multiprocessing.active_children()]


class ChannelsQueue(Queue):
    def getp(self, block=True, timeout=None):
        with self.not_empty:
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time() + timeout
                while not self._qsize():
                    remaining = endtime - time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            item = self._get()
            self.not_full.notify()
            return *item, not self._qsize()

    def peek(self):
        try:
            return self.queue[0]
        except IndexError:
            raise Empty

    def prune_expired(self):
        if self.peek()[0] < time.time():
            self._get()
            return True
        else:
            return False


SyncManager.register(
    "ChannelsQueue",
    ChannelsQueue,
)


sync_executor = ThreadPoolExecutor(max_workers=1)


async def execute_sync(fn, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(sync_executor, fn, *args)


async def queue_aprune_expired(queue: ChannelsQueue):
    return await execute_sync(queue.prune_expired)


async def queue_agetp(queue: ChannelsQueue, block=True, timeout=None):
    return await execute_sync(queue.getp, block, timeout)


async def queue_aget(queue: Queue, block=True, timeout=None):
    return await execute_sync(queue.get, block, timeout)


async def queue_aput(queue: Queue, item, block=True, timeout=None):
    return await execute_sync(queue.put, item, block, timeout)


def _create_or_get_queue(
    manager: SyncManager, d, name, capacity
) -> ChannelsQueue:
    if name not in d:
        d[name] = manager.ChannelsQueue(capacity)
    return d[name]


def _create_or_get_dict(manager, d, name):
    if name not in d:
        d[name] = manager.Dict()
    return d[name]


def _remove_from_groups(groups, channel):
    """
    Removes a channel from all groups. Used when a message on it expires.
    """
    for channels in groups.values():
        if channel in channels:
            del channels[channel]


def _clean_expired(channels, groups, group_expiry):
    """
    Goes through all messages and groups and
    removes those that are expired.
    Any channel with an expired message is removed from all groups.
    """
    # Channel cleanup
    for channel, queue in list(channels.items()):
        # See if it's expired
        try:
            while queue.prune_expired():
                # Any removal prompts group discard
                _remove_from_groups(groups, channel)
        except Empty:
            del channels[channel]

    # Group Expiration
    timeout = int(time.time()) - group_expiry
    for group, channels in groups.items():
        for channel in channels:
            # If join time is older than group_expiry
            # end the group membership
            if (
                groups[group][channel]
                and int(groups[group][channel]) < timeout
            ):
                # Delete from group
                del groups[group][channel]


# based on InMemoryChannelLayer
class MultiprocessingChannelLayer(BaseChannelLayer):
    """
    Multiprocessing channel layer implementation
    """

    def __init__(
        self,
        expiry=60,
        group_expiry=86400,
        capacity=100,
        channel_capacity=None,
        **kwargs,
    ):
        super().__init__(
            expiry=expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
            **kwargs,
        )
        self.group_expiry = group_expiry
        self.manager = SyncManager(ctx=get_context("spawn"))
        self.manager.start()
        self.channels: dict[str, ChannelsQueue] = self.manager.dict()
        self.groups = self.manager.dict()

    # Channel layer API

    extensions = ["groups", "flush"]

    def _send(self, channel, message):
        """
        Send a message onto a (general or specific) channel.
        """
        # Typecheck
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel), "Channel name not valid"
        # If it's a process-local channel, strip off local part and stick full
        # name in message
        assert "__asgi_channel__" not in message

        queue = _create_or_get_queue(
            self.manager, self.channels, channel, self.capacity
        )

        # Add message
        try:
            queue.put(
                queue, (time.time() + self.expiry, deepcopy(message)), False
            )
        except Full:
            raise ChannelFull(channel)

    async def send(self, channel, message):
        """
        Send a message onto a (general or specific) channel.
        """
        # Typecheck
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel), "Channel name not valid"
        # If it's a process-local channel, strip off local part and stick full
        # name in message
        assert "__asgi_channel__" not in message

        queue = _create_or_get_queue(
            self.manager, self.channels, channel, self.capacity
        )

        # Add message
        try:
            await queue_aput(
                queue, (time.time() + self.expiry, deepcopy(message)), False
            )
        except Full:
            raise ChannelFull(channel)

    async def receive(self, channel):
        """
        Receive the first message that arrives on the channel.
        If more than one coroutine waits on the same channel, a random one
        of the waiting coroutines will get the result.
        """
        assert self.valid_channel_name(channel)
        await execute_sync(
            _clean_expired, self.channels, self.groups, self.group_expiry
        )

        queue = _create_or_get_queue(
            self.manager, self.channels, channel, self.capacity
        )

        # Do a plain direct receive
        try:
            _, message, is_empty = await queue_agetp(queue)
        except Empty:
            is_empty = True
        if is_empty:
            del self.channels[channel]

        return message

    async def new_channel(self, prefix="specific."):
        """
        Returns a new channel name that can be used by something in our
        process as a specific channel.
        """
        return "%s.multiprocessing!%s" % (
            prefix,
            "".join(random.choice(string.ascii_letters) for i in range(12)),
        )

    # Expire cleanup

    # Flush extension

    async def flush(self):
        self.channels = self.manager.dict()
        self.groups = self.manager.dict()

    async def close(self):
        await execute_sync(self.manager.shutdown)

    # Groups extension

    async def group_add(self, group, channel):
        """
        Adds the channel name to a group.
        """
        # Check the inputs
        assert self.valid_group_name(group), "Group name not valid"
        assert self.valid_channel_name(channel), "Channel name not valid"
        # Add to group dict
        d = _create_or_get_dict(self.manager, self.groups, group)
        d[channel] = time.time()

    async def group_discard(self, group, channel):
        # Both should be text and valid
        assert self.valid_channel_name(channel), "Invalid channel name"
        assert self.valid_group_name(group), "Invalid group name"
        group_ob = self.groups.get(group, None)
        if group_ob:
            group_ob.pop(channel, None)
            if not group_ob:
                del self.groups[group]

    async def group_send(self, group, message):
        # Check types
        assert isinstance(message, dict), "Message is not a dict"
        assert self.valid_group_name(group), "Invalid group name"
        # Run clean
        execute_sync(
            _clean_expired, self.channels, self.groups, self.group_expiry
        )
        # Send to each channel
        for channel in self.groups.get(group, set()):
            try:
                self.send(channel, message)
            except ChannelFull:
                pass
