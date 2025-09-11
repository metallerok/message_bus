import abc
from message_bus import events
from message_bus.types import Message
from typing import List


class EventHandlerABC(abc.ABC):
    def __init__(self):
        self._emitted_messages = []

    @abc.abstractmethod
    def _handle(self, event: events.Event, context: dict, *args, **kwargs):
        pass

    def handle(self, event: events.Event, context: dict, *args, **kwargs):
        self._before_handle(context)
        try:
            self._handle(event, context=context, *args, **kwargs)
        finally:
            self._after_handle(context)

    def _before_handle(self, context: dict):
        pass

    def _after_handle(self, context: dict):
        pass

    def emmit_message(self, message: Message):
        self._emitted_messages.append(message)

    @property
    def emitted_messages(self) -> List[Message]:
        return self._emitted_messages


class AsyncEventHandlerABC(abc.ABC):
    def __init__(self):
        self._emitted_messages = []

    @abc.abstractmethod
    async def _handle(self, event: events.Event, context: dict, *args, **kwargs):
        pass

    async def handle(self, event: events.Event, context: dict, *args, **kwargs):
        await self._before_handle(context)
        try:
            await self._handle(event, context=context, *args, **kwargs)
        finally:
            await self._after_handle(context)

    async def _before_handle(self, context: dict):
        pass

    async def _after_handle(self, context: dict):
        pass

    def emmit_message(self, message: Message):
        self._emitted_messages.append(message)

    @property
    def emitted_messages(self) -> List[Message]:
        return self._emitted_messages
