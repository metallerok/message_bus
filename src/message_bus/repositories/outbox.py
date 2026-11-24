import abc
from typing import Any, List, Optional, Type


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
    def list_unprocessed(
            self,
            limit: Optional[int] = None,
            exclude_ids: Optional[List[Any]] = None,
    ) -> List:
        """
        Return unprocessed outbox messages.

        Implementations must lock returned rows (SELECT ... FOR UPDATE
        SKIP LOCKED); the lock is held until save() or rollback().
        """
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self):
        """Release row locks and discard partial changes of a failed publish."""
        raise NotImplementedError

    @abc.abstractmethod
    def save(self):
        raise NotImplementedError


class AsyncOutBoxRepoABC(abc.ABC):
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
    async def get(self, id) -> Any:
        raise NotImplementedError

    @abc.abstractmethod
    async def list_unprocessed(
            self,
            limit: Optional[int] = None,
            exclude_ids: Optional[List[Any]] = None,
    ) -> List:
        """
        Return unprocessed outbox messages.

        Implementations must lock returned rows (SELECT ... FOR UPDATE
        SKIP LOCKED); the lock is held until save() or rollback().
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def rollback(self):
        """Release row locks and discard partial changes of a failed publish."""
        raise NotImplementedError

    @abc.abstractmethod
    def save(self):
        raise NotImplementedError
