from message_bus import MessageBus, Event, EventHandlerABC, Command, CommandHandlerABC
from dataclasses import dataclass


@dataclass
class HelloEvent(Event):
    message: str


@dataclass
class GoodbyeEvent(Event):
    message: str


@dataclass
class HelloCommand(Command):
    message: str


@dataclass
class GoodbyeCommand(Command):
    message: str


class HelloEventHandler(EventHandlerABC):
    def handle(self, event: HelloEvent, *args, **kwargs):
        print(f"event: {event.message}")

        self.emmit_message(
            GoodbyeEvent(message="Goodbye, word!")
        )


class GoodByeEventHandler(EventHandlerABC):
    def handle(self, event: HelloEvent, *args, **kwargs):
        print(f"event: {event.message}")


class HelloCommandHandler(CommandHandlerABC):
    def handle(self, cmd: HelloEvent, *args, **kwargs):
        print(f"command: {cmd.message}")

        self.emmit_message(
            GoodbyeEvent(message="Goodbye, word!"),
        )

        self.emmit_message(
            GoodbyeCommand(message="Goodbye, world")
        )


class GoodbyeCommandHandler(CommandHandlerABC):
    def handle(self, cmd: GoodbyeCommand, *args, **kwargs):
        print(f"command: {cmd.message}")


if __name__ == '__main__':
    message_bus = MessageBus()
    message_bus.set_event_handlers(
        event=HelloEvent,
        handlers=[HelloEventHandler(), lambda event: print(f"event from method: {event.message}")]
    )
    message_bus.set_event_handlers(event=GoodbyeEvent, handlers=[GoodByeEventHandler()])
    message_bus.set_command_handler(cmd=HelloCommand, handler=HelloCommandHandler())
    message_bus.set_command_handler(cmd=GoodbyeCommand, handler=GoodbyeCommandHandler())

    message_bus.handle(HelloEvent(message="hello, world"))
    print()
    message_bus.handle(HelloCommand(message="hello, world"))
