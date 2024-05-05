import abc
import datetime as dt
from typing import List
from message_bus.types import Message


class OutBoxRepoABC(abc.ABC):
    def _add(self, outbox_message):
        assert hasattr(outbox_message, "id")
        assert hasattr(outbox_message, "type")
        assert hasattr(outbox_message, "message") and type(getattr(outbox_message, "message")) == Message

        self.add(outbox_message)


    @abc.abstractmethod
    def add(self, outbox_message):
        raise NotImplementedError

    @abc.abstractmethod
    def list_unprocessed(self) -> List:
        raise NotImplementedError
