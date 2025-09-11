import abc
from message_bus import commands
from message_bus.types import Message
from typing import List


class CommandHandlerABC(abc.ABC):
    def __init__(self):
        self._emitted_messages = []

    @abc.abstractmethod
    def handle(self, cmd: commands.Command, context: dict, *args, **kwargs):
        pass

    def emmit_message(self, message: Message):
        self._emitted_messages.append(message)

    @property
    def emitted_messages(self) -> List[Message]:
        return self._emitted_messages


class AsyncCommandHandlerABC(abc.ABC):
    def __init__(self):
        self._emitted_messages = []

    @abc.abstractmethod
    async def handle(self, cmd: commands.Command, context: dict, *args, **kwargs):
        pass

    def emmit_message(self, message: Message):
        self._emitted_messages.append(message)

    @property
    def emitted_messages(self) -> List[Message]:
        return self._emitted_messages
