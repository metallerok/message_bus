import abc


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


class AsyncOutboxHandlerABC(abc.ABC):
    @abc.abstractmethod
    async def _handle(self, outbox_message, context: dict, *args, **kwargs):
        pass

    async def handle(self, outbox_message, context: dict, *args, **kwargs):
        await self._before_handle(context)
        try:
            await self._handle(outbox_message, context=context, *args, **kwargs)
        finally:
            await self._after_handle(context)

    async def _before_handle(self, context: dict):
        pass

    async def _after_handle(self, context: dict):
        pass
