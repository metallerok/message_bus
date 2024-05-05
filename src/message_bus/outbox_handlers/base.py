import abc
from message_bus import events
from message_bus.types import Message
from typing import List


class OutboxHandlerABC(abc.ABC):
    @abc.abstractmethod
    def _handle(self, outbox_message, context: dict, *args, **kwargs):
        pass

    def handle(self, outbox_message, context: dict, *args, **kwargs):
        self._before_handle(context)
        try:
            self._handle(outbox_message, context=context, *args, **kwargs)
        finally:
            self._after_handle(context)

    def _before_handle(self, context: dict):
        pass

    def _after_handle(self, context: dict):
        pass
