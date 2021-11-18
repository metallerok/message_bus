import abc
from typing import List, Union, Callable, Dict, Type, Any
from logging import getLogger

logger = getLogger(__name__)


class Event:
    pass


class Command:
    pass


Message = Union[Event, Command]


class EventHandlerABC(abc.ABC):
    def __init__(self):
        self._emitted_messages = []

    @abc.abstractmethod
    def handle(self, event: Event, *args, **kwargs):
        pass

    def emmit_message(self, message: Message):
        self._emitted_messages.append(message)

    @property
    def emitted_messages(self) -> List[Message]:
        return self._emitted_messages


class CommandHandlerABC(abc.ABC):
    def __init__(self):
        self._emitted_messages = []

    @abc.abstractmethod
    def handle(self, cmd: Command, *args, **kwargs):
        pass

    def emmit_message(self, message: Message):
        self._emitted_messages.append(message)

    @property
    def emitted_messages(self) -> List[Message]:
        return self._emitted_messages


class MessageBusABC(abc.ABC):
    @abc.abstractmethod
    def set_event_handlers(self, event: Type[Event], handlers: List[Union[Callable, EventHandlerABC]]):
        pass

    @abc.abstractmethod
    def set_command_handler(self, cmd: Type[Command], handler: Union[Callable, CommandHandlerABC]):
        pass

    @abc.abstractmethod
    def handle(self, message: Message, *args, **kwargs):
        pass


class MessageBus(MessageBusABC):
    def __init__(
            self,
            event_handlers: Dict[Type[Event], List[Union[Callable, EventHandlerABC]]] = None,
            command_handlers: Dict[Type[Command], Union[Callable, CommandHandlerABC]] = None,
    ):
        if event_handlers:
            self._event_handlers = event_handlers
        else:
            self._event_handlers = dict()

        if command_handlers:
            self._command_handlers = command_handlers
        else:
            self._command_handlers = dict()

    def set_event_handlers(self, event: Type[Event], handlers: List[Union[Callable, EventHandlerABC]]):
        self._event_handlers[event] = handlers

    def set_command_handler(self, cmd: Type[Command], handler: Union[Callable, CommandHandlerABC]):
        self._command_handlers[cmd] = handler

    def handle(self, message: Message, *args, **kwargs) -> List:
        results = []
        queue = [message]

        while queue:
            message = queue.pop(0)

            if isinstance(message, Event):
                events_results = self._handle_event(message, queue, *args, **kwargs)
                results.extend(events_results)
            elif isinstance(message, Command):
                result = self._handle_command(message, queue, *args, **kwargs)
                results.append(result)
            else:
                raise Exception(f"{message} was not an Event or Command type")

        return results

    def _handle_event(self, event: Event, queue: List[Message], *args, **kwargs) -> List[Any]:
        results = []

        for handler in self._event_handlers[type(event)]:
            logger.debug(f"Handling  event {event} with handler {handler}")

            try:
                if issubclass(type(handler), EventHandlerABC):
                    result = handler.handle(event, *args, **kwargs)
                    queue.extend(handler.emitted_messages)
                else:
                    result = handler(event, *args, **kwargs)

                results.append(result)
            except Exception as e:
                logger.exception(f"Error handling event {event}", exc_info=e)
                continue

        return results

    def _handle_command(self, cmd: Command, queue: List[Message], *args, **kwargs) -> Any:
        result = None

        logger.debug(f"Handling command {cmd}")

        try:
            handler = self._command_handlers[type(cmd)]

            if issubclass(type(handler), CommandHandlerABC):
                result = handler.handle(cmd, *args, **kwargs)
                queue.extend(handler.emitted_messages)
            else:
                result = handler(cmd, *args, **kwargs)
        except Exception as e:
            logger.exception(f"Error handling command {cmd}", exc_info=e)
            raise

        return result
