import abc
import logging
import asyncio
from typing_extensions import Optional
import uuid

from typing import (
    Union,
    List,
    Type,
    Dict,
    Callable,
    Any,
    Tuple,
)

from message_bus import events
from message_bus import commands
from message_bus.event_handlers.base import EventHandlerABC
from message_bus.command_handlers.base import CommandHandlerABC
from message_bus.outbox_handlers.base import OutboxHandlerABC
from message_bus.types import Message
from message_bus.repositories.outbox import OutBoxRepoABC

logger = logging.getLogger(__name__)


class MessageBusABC(abc.ABC):
    context = {}

    def __init__(self) -> None:
        self._outbox_handlers: List[OutboxHandlerABC] = []

        super().__init__()

    @abc.abstractmethod
    def set_event_handlers(
            self,
            event: Type[events.Event],
            handlers: List[Union[Callable, EventHandlerABC]],
    ):
        pass

    @abc.abstractmethod
    def get_event_handlers(
            self,
            event: Type[events.Event],
    ) -> List[Union[Callable, EventHandlerABC]]:
        pass

    @abc.abstractmethod
    def set_command_handler(
            self,
            cmd: Type[commands.Command],
            handler: Union[Callable, CommandHandlerABC],
    ):
        pass

    @abc.abstractmethod
    def get_command_handler(
            self,
            command: Type[commands.Command],
    ) -> CommandHandlerABC:
        pass

    @abc.abstractmethod
    def handle(self, message: Message, *args, **kwargs):
        raise NotImplementedError

    def batch_handle(self, messages: List[Message], *args, **kwargs):
        for message in messages:
            self.handle(message, *args, **kwargs)

    @classmethod
    def register_outbox_message(
        cls,
        outbox_repo: OutBoxRepoABC,
        message: Message,
        meta: Optional[dict] = None
    ) -> Any:
        if isinstance(message, commands.Command):
            type_ = "COMMAND"
        elif isinstance(message, events.Event):
            type_ = "EVENT"
        else:
            raise TypeError("Uknown message type")

        model = outbox_repo.get_model()

        outbox_message = model(
            id=uuid.uuid4(),
            type=type_,
            message=type(message).__name__,
            payload=message.serialize(),
        )

        if hasattr(model, "meta"):
            setattr(outbox_message, "meta", meta)

        outbox_repo.add(outbox_message)

        return outbox_message

    def set_outbox_handlers(self, handlers: List[OutboxHandlerABC]):
        self._outbox_handlers = handlers

    def process_outbox(self, outbox_repo: OutBoxRepoABC):
        if len(self._outbox_handlers) == 0:
            return

        outbox_messages = outbox_repo.list_unprocessed()

        for outbox_message in outbox_messages:
            for handler in self._outbox_handlers:
                try:
                    handler.handle(outbox_message, context=self.context)
                    outbox_repo.save()
                except Exception as e:
                    logger.exception(e)
                    break


class MessageBus(MessageBusABC):
    def __init__(
            self,
            event_handlers: Dict[Type[events.Event], List[Union[Callable, EventHandlerABC]]] = None,
            command_handlers: Dict[Type[commands.Command], Union[Callable, CommandHandlerABC]] = None,
    ):
        if event_handlers:
            self._event_handlers = event_handlers
        else:
            self._event_handlers = dict()

        if command_handlers:
            self._command_handlers = command_handlers
        else:
            self._command_handlers = dict()

        self.context = {}

        super().__init__()

    def set_event_handlers(
            self,
            event: Type[events.Event],
            handlers: List[Union[Callable, EventHandlerABC]]
    ):
        self._event_handlers[event] = handlers

    def get_event_handlers(
            self,
            event: Type[events.Event],
    ) -> List[Union[Callable, EventHandlerABC]]:
        return self._event_handlers[event]

    def get_command_handler(
            self,
            command: Type[commands.Command],
    ) -> CommandHandlerABC:
        return self._command_handlers[command]

    def set_command_handler(
            self,
            cmd: Type[commands.Command],
            handler: Union[Callable, CommandHandlerABC],
    ):
        self._command_handlers[cmd] = handler

    def handle(self, message: Message, *args, **kwargs) -> List:
        results = []
        queue = [message]

        while queue:
            message = queue.pop(0)

            if isinstance(message, events.Event):
                events_results = self._handle_event(message, queue, *args, **kwargs)
                results.extend(events_results)
            elif isinstance(message, commands.Command):
                result = self._handle_command(message, queue, *args, **kwargs)
                results.append(result)
            else:
                raise Exception(f"{message} was not an Event or Command type")

        return results

    def _handle_event(
            self,
            event: events.Event,
            queue: List[Message],
            *args, **kwargs
    ) -> List[Any]:
        results = []

        try:
            handlers = self._event_handlers[type(event)]
        except KeyError:
            logger.error(f"Event handlers for {type(event)} does not exist")
            return results

        for handler in handlers:
            logger.debug(f"Handling  event {event} with handler {handler}")

            try:
                if issubclass(type(handler), EventHandlerABC):
                    result = handler.handle(event, context=self.context, *args, **kwargs)
                    queue.extend(handler.emitted_messages)
                else:
                    result = handler(event, context=self.context, *args, **kwargs)

                results.append({
                    "event": event,
                    "result": result
                })
            except Exception as e:
                logger.exception(f"Error handling event {event}", exc_info=e)
                continue

        return results

    def _handle_command(
            self,
            cmd: commands.Command,
            queue: List[Message],
            *args, **kwargs
    ) -> Any:
        logger.debug(f"Handling command {cmd}")

        try:
            handler = self._command_handlers[type(cmd)]

            if issubclass(type(handler), CommandHandlerABC):
                result = handler.handle(cmd, context=self.context, *args, **kwargs)
                queue.extend(handler.emitted_messages)
            else:
                result = handler(cmd, context=self.context, *args, **kwargs)
        except Exception as e:
            logger.exception(f"Error handling command {cmd}", exc_info=e)
            raise

        return {
            "command": cmd,
            "result": result,
        }


class AsyncMessageBus(MessageBusABC):
    def __init__(
            self,
            event_handlers: Dict[Type[events.Event], List[Union[Callable, EventHandlerABC]]] = None,
            command_handlers: Dict[Type[commands.Command], Union[Callable, CommandHandlerABC]] = None,
    ):
        if event_handlers:
            self._event_handlers = event_handlers
        else:
            self._event_handlers = dict()

        if command_handlers:
            self._command_handlers = command_handlers
        else:
            self._command_handlers = dict()

        self.context = {}

        super().__init__()

    def set_event_handlers(
            self,
            event: Type[events.Event],
            handlers: List[Union[Callable, EventHandlerABC]]
    ):
        self._event_handlers[event] = handlers

    def set_command_handler(
            self,
            cmd: Type[commands.Command],
            handler: Union[Callable, CommandHandlerABC],
    ):
        self._command_handlers[cmd] = handler

    def get_event_handlers(
            self,
            event: Type[events.Event],
    ) -> List[Union[Callable, EventHandlerABC]]:
        return self._event_handlers[event]

    def get_command_handler(
            self,
            command: Type[commands.Command],
    ) -> CommandHandlerABC:
        return self._command_handlers[command]

    async def process_outbox(self, outbox_repo: OutBoxRepoABC):
        if len(self._outbox_handlers) == 0:
            return

        outbox_messages = await outbox_repo.list_unprocessed()

        for outbox_message in outbox_messages:
            for handler in self._outbox_handlers:
                try:
                    await handler.handle(outbox_message, context=self.context)
                    await outbox_repo.save()
                except Exception as e:
                    logger.exception(e)
                    break

    async def batch_handle(self, messages: List[Message], *args, **kwargs):
        for message in messages:
            await self.handle(message, *args, **kwargs)

    async def handle(self, message: Message, *args, **kwargs) -> List:
        results = []
        queue = [message]

        while queue:
            message = queue.pop(0)

            if isinstance(message, events.Event):
                events_results = await self._handle_event(message, queue, *args, **kwargs)
                results.extend(events_results)
            elif isinstance(message, commands.Command):
                result = self._handle_command(message, queue, *args, **kwargs)
                results.append(result)
            else:
                raise Exception(f"{message} was not an Event or Command type")

        return results

    async def _handle_event(
            self,
            event: events.Event,
            queue: List[Message],
            *args, **kwargs
    ) -> Tuple[Any]:
        coroutines = []

        try:
            handlers = self._event_handlers[type(event)]
        except KeyError:
            logger.error(f"Event handlers for {type(event)} does not exist")
            return tuple()

        for handler in handlers:
            logger.debug(f"Handling event {event} with handler {handler}")

            try:
                if issubclass(type(handler), EventHandlerABC):
                    coroutine = handler.handle(event, context=self.context, *args, **kwargs)
                    coroutines.append(coroutine)
                    queue.extend(handler.emitted_messages)
                else:
                    coroutine = handler(event, context=self.context, *args, **kwargs)
                    coroutines.append(coroutine)

            except Exception as e:
                logger.exception(f"Error handling event {event}", exc_info=e)
                continue

        try:
            results = tuple(await asyncio.gather(*coroutines))
        except Exception as e:
            logger.exception("Error handling events", exc_info=e)
            return tuple()

        if "db_session" in self.context:
            self.context["db_session"].close()

        return results

    def _handle_command(
            self,
            cmd: commands.Command,
            queue: List[Message],
            *args, **kwargs
    ) -> Any:
        logger.debug(f"Handling command {cmd}")

        try:
            handler = self._command_handlers[type(cmd)]

            if issubclass(type(handler), CommandHandlerABC):
                result = handler.handle(cmd, context=self.context, *args, **kwargs)
                queue.extend(handler.emitted_messages)
            else:
                result = handler(cmd, context=self.context, *args, **kwargs)
        except Exception as e:
            logger.exception(f"Error handling command {cmd}", exc_info=e)
            raise

        return result
