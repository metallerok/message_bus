# Message Bus Documentation

## Overview

The Message Bus is a flexible command and event handling system that provides:

- **Command/Event Processing**: Handle business logic through commands and events
- **Synchronous & Asynchronous Support**: Both sync and async implementations available
- **Outbox Pattern Integration**: Automatically persist messages to outbox before processing
- **Transaction Safety**: Ensure message persistence within database transactions
- **Handler Composition**: Support for emitting new messages from handlers

## Architecture

### Core Components

1. **MessageBusABC** - Abstract base class for message bus implementations
2. **AsyncMessageBusABC** - Abstract base class for async message bus implementations
3. **MessageBus** - Concrete synchronous implementation
4. **AsyncMessageBus** - Concrete asynchronous implementation
5. **Outbox Repository System** - Handles persistence of messages for outbox pattern
6. **Handler Base Classes** - For commands, events, and outbox handlers

### Message Types

- **Command**: Represents an action to be performed (e.g., "CreateOrder")
- **Event**: Represents something that happened in the system (e.g., "OrderCreated")
- **Outbox Message**: A persisted version of commands/events for processing

## Quick Start

### Basic Setup

```python
from message_bus import MessageBus
from message_bus.commands import Command
from message_bus.events import Event

# Define your commands and events
class ProcessOrder(Command):
    order_id: int

class OrderProcessed(Event):
    order_id: int
    status: str

# Create message bus
message_bus = MessageBus()
```

### Registering Handlers

```python
from message_bus.command_handlers.base import CommandHandlerABC
from message_bus.event_handlers.base import EventHandlerABC

class ProcessOrderHandler(CommandHandlerABC):
    def handle(self, cmd: ProcessOrder, context: dict, *args, **kwargs):
        # Process the order
        print(f"Processing order {cmd.order_id}")

        # Emit an event when done
        self.emmit_message(OrderProcessed(
            order_id=cmd.order_id,
            status="completed"
        ))

class OrderProcessedHandler(EventHandlerABC):
    def _handle(self, event: OrderProcessed, context: dict, *args, **kwargs):
        print(f"Order {event.order_id} was processed with status: {event.status}")

# Register handlers
message_bus.set_command_handler(ProcessOrder, ProcessOrderHandler())
message_bus.set_event_handlers(OrderProcessed, [OrderProcessedHandler()])
```

### Using Outbox Pattern

```python
from message_bus.repositories.outbox import OutBoxRepoABC
from message_bus.core import AsyncMessageBus

# Define your outbox repository (implementation depends on your ORM)
class SAOutBoxRepo(OutBoxRepoABC):
    def __init__(self, db_session):
        self._db_session = db_session
        super().__init__()

    def get_model(self):
        return OutBox  # Your database model

    def _add(self, outbox_message):
        self._db_session.add(outbox_message)

    async def list_unprocessed(self):
        # Implementation depends on your database
        pass

# Create message bus with outbox support
message_bus = AsyncMessageBus()

# Register outbox handler (e.g., for sending to message queue)
class MessageQueueHandler(OutboxHandlerABC):
    def _handle(self, outbox_message, context: dict, *args, **kwargs):
        # Send message to external queue or service
        print(f"Sending {outbox_message.message} to queue")

# Register your outbox handler
message_bus.set_outbox_handlers([MessageQueueHandler()])

# Usage in transaction
order = create_order()

# Register the outbox message
message_bus.register_outbox_message(
    outbox_repo,
    message=ProcessOrder(order_id=order.id)
)

# Commit transaction to persist both order and outbox message
db_session.commit()

# Process outbox messages (usually done in background job)
await message_bus.process_outbox(outbox_repo)
```

## Outbox Pattern

The outbox pattern ensures that messages are persisted to a database table before they're processed. This provides:

1. **Transaction Safety**: Messages are stored within the same transaction as business logic
2. **Idempotency**: If processing fails, messages can be retried
3. **Reliability**: Ensures no messages are lost during processing

### Outbox Repository Interface

```python
from message_bus.repositories.outbox import OutBoxRepoABC

class MyOutBoxRepo(OutBoxRepoABC):
    def get_model(self):
        # Return your database model class
        return MyOutBoxModel

    def _add(self, outbox_message):
        # Add to database session
        self._db_session.add(outbox_message)

    def list_unprocessed(self):
        # Return list of unprocessed messages
        pass

    def save(self):
        # Commit database changes
        self._db_session.commit()
```

## Async Usage

```python
import asyncio
from message_bus import AsyncMessageBus

async def handle_async_messages():
    message_bus = AsyncMessageBus()

    # Register async handlers
    message_bus.set_command_handler(ProcessOrder, AsyncProcessOrderHandler())

    # Handle messages asynchronously
    result = await message_bus.handle(ProcessOrder(order_id=123))

    # Process outbox messages asynchronously
    await message_bus.process_outbox(outbox_repo)
```

## Component Details

### Message Classes

All messages must inherit from either `Command` or `Event`:

```python
from message_bus.types import Message

class MyCommand(Command):
    def __init__(self, data: str):
        self.data = data

class MyEvent(Event):
    def __init__(self, data: str):
        self.data = data
```

### Handler Patterns

#### Command Handlers

```python
class MyCommandHandler(CommandHandlerABC):
    def handle(self, cmd: Command, context: dict, *args, **kwargs):
        # Process command logic
        return "result"
```

#### Event Handlers

```python
class MyEventHandler(EventHandlerABC):
    def _handle(self, event: Event, context: dict, *args, **kwargs):
        # Process event logic
        pass
```

#### Outbox Handlers

```python
class MyOutboxHandler(OutboxHandlerABC):
    def _handle(self, outbox_message, context: dict, *args, **kwargs):
        # Process outbox message (e.g., send to queue)
        pass
```

## Configuration and Best Practices

1. **Transaction Safety**: Always commit database transactions after registering outbox messages
2. **Background Processing**: Process outbox messages in background jobs or separate processes
3. **Error Handling**: Implement proper error handling in outbox handlers to avoid message loss
4. **Idempotency**: Design handlers to be idempotent since messages may be retried

## Core Features

### Message Serialization

The system uses `dataclasses_serialization` for serializing messages to/from database format:

```python
from message_bus.types import Message

class MyMessage(Message):
    name: str
    value: int

    def serialize(self) -> dict:
        return super().serialize()

    @classmethod
    def deserialize(cls, data: dict) -> 'MyMessage':
        return super().deserialize(data)
```

### Context Support

All handlers have access to a shared context:

```python
message_bus.context["db_session"] = session
result = message_bus.handle(my_command)
```

### Batch Processing

```python
# Process multiple messages at once
messages = [cmd1, cmd2, cmd3]
results = message_bus.batch_handle(messages)
```

## License

MIT License - see LICENSE file for details.