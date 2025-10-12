"""
Message Bus - A flexible command and event handling system with outbox pattern support.

This module provides a complete message bus implementation that supports:
- Command/Event processing
- Synchronous and asynchronous operations
- Outbox pattern for reliable message handling
- Handler composition with message emission
- Transaction-safe message persistence

The system is designed to be extensible and works well with database transactions
to ensure that messages are persisted before processing.

Usage Examples:
    from message_bus import MessageBus
    from message_bus.commands import Command
    from message_bus.events import Event

    class ProcessOrder(Command):
        order_id: int

    class OrderProcessed(Event):
        order_id: int
        status: str

    message_bus = MessageBus()

    # Register handlers
    message_bus.set_command_handler(ProcessOrder, ProcessOrderHandler())
    message_bus.set_event_handlers(OrderProcessed, [OrderProcessedHandler()])

    # Handle messages
    result = message_bus.handle(ProcessOrder(order_id=123))
"""

from message_bus.core import MessageBus, MessageBusABC, AsyncMessageBus  # noqa
from message_bus.commands import Command  # noqa
from message_bus.events import Event  # noqa
from message_bus.types import Message  # noqa

__all__ = [
    "MessageBus",
    "AsyncMessageBus",
    "MessageBusABC",
    "Command",
    "Event",
    "Message"
]
