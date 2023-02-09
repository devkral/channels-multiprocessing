from copy import deepcopy
from channels.layers import BaseChannelLayer
from channels.exceptions import ChannelFull
from multiprocessing.managers import SyncManager
from multiprocessing import get_context
import time
import random
import string
from queue import Queue, Full, Empty
from concurrent.futures import ThreadPoolExecutor
import asyncio


class ChannelsMultiprocessingQueue(Queue):
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
                endtime = time.time() + timeout
                while not self._qsize():
                    remaining = endtime - time.time()
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

    def prune_expired(self, timeout):
        if self.peek()[0] < timeout:
            self._get()
            return True
        else:
            return False


SyncManager.register(
    "ChannelsMultiprocessingQueue",
    ChannelsMultiprocessingQueue,
)


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
        mp_context=None,
        **kwargs,
    ):
        super().__init__(
            expiry=expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
            **kwargs,
        )
        self.active = True
        self.group_expiry = group_expiry
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.manager = SyncManager(ctx=get_context(mp_context))
        self.manager.start()
        self.channels: dict[
            str, ChannelsMultiprocessingQueue
        ] = self.manager.dict()
        self.groups = self.manager.dict()

    async def execute_as_async(self, fn, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, fn, *args)

    def _create_or_get_channel(self, name) -> ChannelsMultiprocessingQueue:
        return self.channels.setdefault(
            name,
            self.manager.ChannelsMultiprocessingQueue(self.get_capacity(name)),
        )

    def _create_or_get_dict(self, d, name):
        return d.setdefault(name, self.manager.dict())

    def _remove_from_groups(self, channel):
        """
        Removes a channel from all groups. Used when a message on it expires.
        """
        for channels in self.groups.values():
            channels.pop(channel, None)

    def _clean_expired(self):
        """
        Goes through all messages and groups and
        removes those that are expired.
        Any channel with an expired message is removed from all groups.
        """
        if not self.active:
            return
        # Channel cleanup
        timeout = time.time()
        for channel, queue in list(self.channels.items()):
            # See if it's expired
            try:
                while queue.prune_expired(timeout):
                    # Any removal prompts group discard
                    self._remove_from_groups(channel)
            except Empty:
                self.channels.pop(channel, None)

        # Group Expiration
        timeout = time.time() - self.group_expiry
        for channels in self.groups.values():

            for name, timestamp in list(channels.items()):
                # If join time is older than group_expiry
                # end the group membership
                if timestamp and timestamp < timeout:
                    # Delete from group
                    channels.pop(name, None)

    # Channel layer API

    extensions = ["groups", "flush"]

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

        queue = self._create_or_get_channel(channel)

        # Add message
        try:
            await self.execute_as_async(
                queue.put,
                (time.time() + self.expiry, deepcopy(message)),
                False,
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
        await self.execute_as_async(self._clean_expired)

        queue = self._create_or_get_channel(channel)

        # Do a plain direct receive
        try:
            _, message, is_empty = await asyncio.wait_for(
                self.execute_as_async(queue.getp), self.expiry
            )
        except (Empty, asyncio.CancelledError, asyncio.TimeoutError) as exc:
            # we need to clean it up here as we reraise the exception
            self.channels.pop(channel, None)
            raise exc
        # second code path to clean it up
        if is_empty:
            self.channels.pop(channel, None)
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
        self.channels.clear()
        self.groups.clear()

    async def close(self):
        if self.active:
            self.active = False
            self.manager.shutdown()
            self.executor.shutdown(False, cancel_futures=True)

    # Groups extension

    async def group_add(self, group, channel):
        """
        Adds the channel name to a group.
        """
        # Check the inputs
        assert self.valid_group_name(group), "Group name not valid"
        assert self.valid_channel_name(channel), "Channel name not valid"
        # Add to group dict
        d = self._create_or_get_dict(self.groups, group)
        d[channel] = time.time()

    async def group_discard(self, group, channel):
        # Both should be text and valid
        assert self.valid_channel_name(channel), "Invalid channel name"
        assert self.valid_group_name(group), "Invalid group name"
        group_ob = self.groups.get(group, None)
        if group_ob:
            group_ob.pop(channel, None)
            # group is now empty
            if not group_ob:
                self.groups.pop(group, None)

    async def group_send(self, group, message):
        # Check types
        assert isinstance(message, dict), "Message is not a dict"
        assert self.valid_group_name(group), "Invalid group name"
        # Run clean
        await self.execute_as_async(self._clean_expired)
        # Send to each channel
        ops = []

        if group in self.groups:
            for channel in self.groups[group].keys():
                ops.append(asyncio.ensure_future(self.send(channel, message)))
        await asyncio.wait(ops)
