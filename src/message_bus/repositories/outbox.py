import abc
import datetime as dt
from dataclasses import dataclass
from message_bus.types import Message


@dataclass
class OutBoxMessageABC(abc.ABC):
    id: str
    type: str
    message: Message
    processed: dt.datetime


class OutBoxRepoABC(abc.ABC):
    @abc.abstractmethod
    def add(self, outbox_message: OutBoxMessageABC):
        raise NotImplementedError
