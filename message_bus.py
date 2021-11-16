import abc
from typing import List, Union, Callable, Dict, Type


class Event:
    pass


class HandlerABC(abc.ABC):
    @abc.abstractmethod
    def handle(self, event: Event, *args, **kwargs):
        pass


class MessageBusABC(abc.ABC):
    @abc.abstractmethod
    def set_handlers(self, event: Type[Event], handlers: List[Union[Callable, HandlerABC]]):
        pass


class MessageBus(MessageBusABC):
    def __init__(self, handlers: Dict[Type[Event], List[Union[Callable, HandlerABC]]] = None):
        if handlers:
            self._handlers = handlers
        else:
            self._handlers = dict()

    def set_handlers(self, event: Type[Event], handlers: List[Union[Callable, HandlerABC]]):
        self._handlers[event] = handlers

    def handle(self, event: Event, *args, **kwargs) -> List:
        results = []
        queue = [event]

        while queue:
            event = queue.pop(0)

            for handler in self._handlers[type(event)]:
                if issubclass(type(handler), HandlerABC):
                    result = handler.handle(event, *args, **kwargs)
                else:
                    result = handler(event, *args, **kwargs)

                results.append(result)

        return results


