from copy import deepcopy
from channels.layers import BaseChannelLayer
from channels.exceptions import ChannelFull
from multiprocessing.managers import SyncManager
from multiprocessing import get_context
import time
import random
import string
from queue import Queue, Full, Empty
from asgiref.sync import sync_to_async


def peek_queue(queue):
    try:
        return queue.queue[0]
    except IndexError:
        raise Empty


class MultiprocessingQueue(Queue):
    def __init__(self, manager, maxsize: int = ...) -> None:
        self.maxsize = maxsize
        self._init(maxsize)
        self.mutex = manager.Lock()

        # Notify not_empty whenever an item is added to the queue; a
        # thread waiting to get is notified then.
        self.not_empty = manager.Condition(self.mutex)

        # Notify not_full whenever an item is removed from the queue;
        # a thread waiting to put is notified then.
        self.not_full = manager.Condition(self.mutex)

        # Notify all_tasks_done whenever the number of unfinished tasks
        # drops to zero; thread waiting to join() is notified to resume
        self.all_tasks_done = manager.Condition(self.mutex)
        self.unfinished_tasks = 0

    def prune_expired(self):
        with self.not_empty:
            if not self._qsize():
                raise Empty
            if self.queue[0][0] < time.time():
                self._get()
                return True
            else:
                return False

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
        self.channels: dict[str, MultiprocessingQueue] = self.manager.dict()
        self.groups = self.manager.dict()

    # Channel layer API

    extensions = ["groups", "flush"]

    @sync_to_async
    def send(self, channel, message):
        """
        Send a message onto a (general or specific) channel.
        """
        # Typecheck
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel), "Channel name not valid"
        # If it's a process-local channel, strip off local part and stick full
        # name in message
        assert "__asgi_channel__" not in message

        queue = self.channels.setdefault(
            channel, MultiprocessingQueue(self.manager, self.capacity)
        )

        # Add message
        try:
            queue.put((time.time() + self.expiry, deepcopy(message)), False)
        except Full:
            raise ChannelFull(channel)

    @sync_to_async
    def receive(self, channel):
        """
        Receive the first message that arrives on the channel.
        If more than one coroutine waits on the same channel, a random one
        of the waiting coroutines will get the result.
        """
        assert self.valid_channel_name(channel)
        self._clean_expired()

        queue = self.channels.setdefault(
            channel, MultiprocessingQueue(self.manager, self.capacity)
        )

        # Do a plain direct receive
        try:
            _, message, is_empty = queue.getp()
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

    def _clean_expired(self):
        """
        Goes through all messages and groups and
        removes those that are expired.
        Any channel with an expired message is removed from all groups.
        """
        # Channel cleanup
        for channel, queue in list(self.channels.items()):
            # See if it's expired
            try:
                while queue.prune_expired():
                    # Any removal prompts group discard
                    self._remove_from_groups(channel)
            except Empty:
                del self.channels[channel]

        # Group Expiration
        timeout = int(time.time()) - self.group_expiry
        for group, channels in self.groups.items():
            for channel in channels:
                # If join time is older than group_expiry
                # end the group membership
                if (
                    self.groups[group][channel]
                    and int(self.groups[group][channel]) < timeout
                ):
                    # Delete from group
                    del self.groups[group][channel]

    # Flush extension

    @sync_to_async
    def flush(self):
        self.channels = self.manager.dict()
        self.groups = self.manager.dict()

    @sync_to_async
    def close(self):
        self.manager.shutdown()

    def _remove_from_groups(self, channel):
        """
        Removes a channel from all groups. Used when a message on it expires.
        """
        for channels in self.groups.values():
            if channel in channels:
                del channels[channel]

    # Groups extension

    @sync_to_async
    def group_add(self, group, channel):
        """
        Adds the channel name to a group.
        """
        # Check the inputs
        assert self.valid_group_name(group), "Group name not valid"
        assert self.valid_channel_name(channel), "Channel name not valid"
        # Add to group dict
        d = self.groups.setdefault(group, self.manager.dict())
        d[channel] = time.time()

    @sync_to_async
    def group_discard(self, group, channel):
        # Both should be text and valid
        assert self.valid_channel_name(channel), "Invalid channel name"
        assert self.valid_group_name(group), "Invalid group name"
        # Remove from group set
        group_ob = self.groups.get(group, None)
        if group_ob:
            group_ob.pop(channel, None)
            if not group_ob:
                del self.groups[group]

    @sync_to_async
    def group_send(self, group, message):
        # Check types
        assert isinstance(message, dict), "Message is not a dict"
        assert self.valid_group_name(group), "Invalid group name"
        # Run clean
        self._clean_expired()
        # Send to each channel
        for channel in self.groups.get(group, set()):
            try:
                self.send.func(channel, message)
            except ChannelFull:
                pass
