import abc
from typing import List, Type, Any


class OutBoxRepoABC(abc.ABC):
    @abc.abstractmethod
    def get_model(self) -> Type:
        raise NotImplementedError

    def add(self, outbox_message):
        assert hasattr(outbox_message, "id")
        assert hasattr(outbox_message, "type")
        assert hasattr(outbox_message, "message")
        assert hasattr(outbox_message, "payload")

        self._add(outbox_message)

    @abc.abstractmethod
    def _add(self, outbox_message):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, id) -> Any:
        raise NotImplementedError

    @abc.abstractmethod
    def list_unprocessed(self) -> List:
        raise NotImplementedError

    @abc.abstractmethod
    def save(self):
        raise NotImplementedError
