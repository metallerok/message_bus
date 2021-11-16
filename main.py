from message_bus import MessageBus, Event, HandlerABC
from typing import List
from dataclasses import dataclass


@dataclass
class HelloEvent(Event):
    message: str


@dataclass
class GoodbyeEvent(Event):
    message: str


class HelloHandler(HandlerABC):
    def handle(self, event: HelloEvent, *args, **kwargs):
        print(event.message)

        self.emmit_event(
            GoodbyeEvent(message="Goodbye, word!")
        )


class GoodByeHandler(HandlerABC):
    def handle(self, event: HelloEvent, *args, **kwargs):
        print(event.message)


if __name__ == '__main__':
    message_bus = MessageBus()
    message_bus.set_handlers(
        event=HelloEvent,
        handlers=[HelloHandler(), lambda event: print(f"from method: {event.message}")]
    )
    message_bus.set_handlers(
        event=GoodbyeEvent,
        handlers=[GoodByeHandler()]
    )

    message_bus.handle(HelloEvent(message="hello_world"))
