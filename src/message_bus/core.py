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
from message_bus.outbox_handlers.base import OutboxHandlerABC, AsyncOutboxHandlerABC
from message_bus.types import Message
from message_bus.repositories.outbox import OutBoxRepoABC, AsyncOutBoxRepoABC

logger = logging.getLogger(__name__)


class MessageBusABC(abc.ABC):
    """
    Abstract base class for message bus implementations.

    The message bus provides:
    - Command/Event handling with dedicated handlers
    - Support for the outbox pattern (message persistence before processing)
    - Context support for shared state across handlers
    - Handler composition with message emission capabilities

    Example:
        ```python
        from message_bus import MessageBus

        message_bus = MessageBus()

        # Register handlers for commands and events
        message_bus.set_command_handler(MyCommand, MyCommandHandler())
        message_bus.set_event_handlers(MyEvent, [MyEventHandler()])
        ```
    """

    context = {}

    def __init__(self) -> None:
        self._outbox_handlers: List[OutboxHandlerABC] = []
        super().__init__()

    @abc.abstractmethod
    def set_event_handlers(
            self,
            event: Type[events.Event],
            handlers: List[Union[Callable, EventHandlerABC]],
    ) -> None:
        """
        Register event handlers for a specific event type.

        Args:
            event: The event type to register handlers for
            handlers: List of event handlers (callable or EventHandlerABC instances)
        """
        pass

    @abc.abstractmethod
    def get_event_handlers(
            self,
            event: Type[events.Event],
    ) -> List[Union[Callable, EventHandlerABC]]:
        """
        Get registered event handlers for a specific event type.

        Args:
            event: The event type to get handlers for

        Returns:
            List of registered event handlers
        """
        pass

    @abc.abstractmethod
    def set_command_handler(
            self,
            cmd: Type[commands.Command],
            handler: Union[Callable, CommandHandlerABC],
    ) -> None:
        """
        Register a handler for a specific command type.

        Args:
            cmd: The command type to register handler for
            handler: Command handler (callable or CommandHandlerABC instance)
        """
        pass

    @abc.abstractmethod
    def get_command_handler(
        self,
        command: Type[commands.Command],
    ) -> Optional[Union[Callable, CommandHandlerABC]]:
        """
        Get registered handler for a specific command type.

        Args:
            command: The command type to get handler for

        Returns:
            Registered command handler or None if not found
        """
        pass

    @abc.abstractmethod
    def handle(self, message: Message, *args, **kwargs) -> List:
        """
        Handle a message (command or event).

        Args:
            message: The message to handle
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            List of results from handlers
        """
        raise NotImplementedError

    def batch_handle(self, messages: List[Message], *args, **kwargs) -> List:
        """
        Handle multiple messages in batch.

        Args:
            messages: List of messages to handle
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            List of results from handling all messages
        """
        results = []
        for message in messages:
            result = self.handle(message, *args, **kwargs)
            results.extend(result)
        return results

    @classmethod
    def register_outbox_message(
        cls,
        outbox_repo: OutBoxRepoABC,
        message: Message,
        id: Optional[uuid.UUID] = None,
        meta: Optional[dict] = None
    ) -> Any:
        """
        Register a message to be persisted in the outbox repository.

        This method creates an outbox entry for a message that will be processed later,
        ensuring it's persisted before processing begins.

        Args:
            outbox_repo: Repository for storing outbox messages
            message: The message to register in the outbox
            id: Optional UUID for the outbox message (auto-generated if None)
            meta: Optional metadata to store with the outbox message

        Returns:
            The created outbox message instance
        """
        if isinstance(message, commands.Command):
            type_ = "COMMAND"
        elif isinstance(message, events.Event):
            type_ = "EVENT"
        else:
            raise TypeError("Unknown message type")

        model = outbox_repo.get_model()

        outbox_message = model(
            id=id or uuid.uuid4(),
            type=type_,
            message=type(message).__name__,
            payload=message.serialize(),
        )

        if hasattr(model, "meta"):
            setattr(outbox_message, "meta", meta)

        outbox_repo.add(outbox_message)

        return outbox_message

    def set_outbox_handlers(self, handlers: List[OutboxHandlerABC]) -> None:
        """
        Register outbox handlers for processing messages from the outbox.

        Args:
            handlers: List of outbox handlers to register
        """
        self._outbox_handlers = handlers

    def process_outbox(self, outbox_repo: OutBoxRepoABC) -> None:
        """
        Process all unprocessed messages from the outbox repository.

        This method should be called periodically to process messages that have been
        persisted to the outbox but not yet processed.

        Args:
            outbox_repo: Repository containing outbox messages to process
        """
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


class AsyncMessageBusABC(abc.ABC):
    """
    Abstract base class for asynchronous message bus implementations.

    This interface extends MessageBusABC to support asynchronous operations,
    providing the same functionality as its synchronous counterpart but with
    async/await capabilities.

    Example:
        ```python
        from message_bus import AsyncMessageBus

        async_message_bus = AsyncMessageBus()

        # Register async handlers for commands and events
        async_message_bus.set_command_handler(MyCommand, MyAsyncCommandHandler())
        async_message_bus.set_event_handlers(MyEvent, [MyAsyncEventHandler()])
        ```
    """

    context = {}

    def __init__(self) -> None:
        self._outbox_handlers: List[AsyncOutboxHandlerABC] = []
        super().__init__()

    @abc.abstractmethod
    def set_event_handlers(
            self,
            event: Type[events.Event],
            handlers: List[Union[Callable, EventHandlerABC]],
    ) -> None:
        """
        Register event handlers for a specific event type.

        Args:
            event: The event type to register handlers for
            handlers: List of event handlers (callable or EventHandlerABC instances)
        """
        pass

    @abc.abstractmethod
    def get_event_handlers(
            self,
            event: Type[events.Event],
    ) -> List[Union[Callable, EventHandlerABC]]:
        """
        Get registered event handlers for a specific event type.

        Args:
            event: The event type to get handlers for

        Returns:
            List of registered event handlers
        """
        pass

    @abc.abstractmethod
    def set_command_handler(
            self,
            cmd: Type[commands.Command],
            handler: Union[Callable, CommandHandlerABC],
    ) -> None:
        """
        Register a handler for a specific command type.

        Args:
            cmd: The command type to register handler for
            handler: Command handler (callable or CommandHandlerABC instance)
        """
        pass

    @abc.abstractmethod
    def get_command_handler(
        self,
        command: Type[commands.Command],
    ) -> Optional[Union[Callable, CommandHandlerABC]]:
        """
        Get registered handler for a specific command type.

        Args:
            command: The command type to get handler for

        Returns:
            Registered command handler or None if not found
        """
        pass

    @abc.abstractmethod
    async def handle(self, message: Message, *args, **kwargs) -> List:
        """
        Handle a message (command or event) asynchronously.

        Args:
            message: The message to handle
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            List of results from handlers
        """
        raise NotImplementedError

    async def batch_handle(self, messages: List[Message], *args, **kwargs) -> List:
        """
        Handle multiple messages in batch asynchronously.

        Args:
            messages: List of messages to handle
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            List of results from handling all messages
        """
        max_concurrency = kwargs.pop("max_concurrency", 5)
        q: asyncio.Queue[Tuple[int, Any]] = asyncio.Queue()
        results: List[Any] = [None] * len(messages)

        for idx, msg in enumerate(messages):
            q.put_nowait((idx, msg))

        async def worker():
            while True:
                try:
                    idx, msg = await q.get()
                except asyncio.CancelledError:
                    break
                try:
                    res = await self.handle(msg, *args, **kwargs)
                    results[idx] = res
                except Exception as e:
                    results[idx] = e
                finally:
                    q.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(max_concurrency)]
        try:
            await q.join()
        finally:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        for msg, res in zip(messages, results):
            if isinstance(res, Exception):
                logger.error("Error processing message %r: %s", msg, res)

        return [r for r in results if not isinstance(r, Exception)]

    @classmethod
    def register_outbox_message(
        cls,
        outbox_repo: AsyncOutBoxRepoABC,
        message: Message,
        id: Optional[uuid.UUID] = None,
        meta: Optional[dict] = None
    ) -> Any:
        """
        Register a message to be persisted in the outbox repository (async version).

        This method creates an outbox entry for a message that will be processed later,
        ensuring it's persisted before processing begins.

        Args:
            outbox_repo: Repository for storing outbox messages
            message: The message to register in the outbox
            id: Optional UUID for the outbox message (auto-generated if None)
            meta: Optional metadata to store with the outbox message

        Returns:
            The created outbox message instance
        """
        if isinstance(message, commands.Command):
            type_ = "COMMAND"
        elif isinstance(message, events.Event):
            type_ = "EVENT"
        else:
            raise TypeError("Unknown message type")

        model = outbox_repo.get_model()

        outbox_message = model(
            id=id or uuid.uuid4(),
            type=type_,
            message=type(message).__name__,
            payload=message.serialize(),
        )

        if hasattr(model, "meta"):
            setattr(outbox_message, "meta", meta)

        outbox_repo.add(outbox_message)

        return outbox_message

    def set_outbox_handlers(self, handlers: List[AsyncOutboxHandlerABC]) -> None:
        """
        Register outbox handlers for processing messages from the outbox.

        Args:
            handlers: List of async outbox handlers to register
        """
        self._outbox_handlers = handlers

    async def process_outbox(self, outbox_repo: AsyncOutBoxRepoABC) -> None:
        """
        Process all unprocessed messages from the outbox repository asynchronously.

        This method should be called periodically to process messages that have been
        persisted to the outbox but not yet processed.

        Args:
            outbox_repo: Repository containing outbox messages to process
        """
        if len(self._outbox_handlers) == 0:
            return

        outbox_messages = await outbox_repo.list_unprocessed()

        for outbox_message in outbox_messages:
            for handler in self._outbox_handlers:
                try:
                    await handler.handle(outbox_message, context=self.context)
                    outbox_repo.save()
                except Exception as e:
                    logger.exception(e)
                    break


class MessageBus(MessageBusABC):
    """
    Concrete implementation of the synchronous message bus.

    This class provides a complete implementation for handling commands and events
    in a synchronous manner, with support for outbox pattern integration.

    Example:
        ```python
        from message_bus import MessageBus

        # Create and configure message bus
        message_bus = MessageBus(
            event_handlers={MyEvent: [MyEventHandler()]},
            command_handlers={MyCommand: MyCommandHandler()}
        )

        # Handle messages
        result = message_bus.handle(MyCommand(data="test"))
        ```
    """

    def __init__(
            self,
            event_handlers: Optional[Dict[Type[events.Event], List[Union[Callable, EventHandlerABC]]]] = None,
            command_handlers: Optional[Dict[Type[commands.Command], Union[Callable, CommandHandlerABC]]] = None,
    ):
        """
        Initialize the message bus with optional pre-configured handlers.

        Args:
            event_handlers: Optional mapping of event types to their handlers
            command_handlers: Optional mapping of command types to their handlers
        """
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
    ) -> None:
        """
        Register event handlers for a specific event type.

        Args:
            event: The event type to register handlers for
            handlers: List of event handlers (callable or EventHandlerABC instances)
        """
        self._event_handlers[event] = handlers

    def get_event_handlers(
            self,
            event: Type[events.Event],
    ) -> List[Union[Callable, EventHandlerABC]]:
        """
        Get registered event handlers for a specific event type.

        Args:
            event: The event type to get handlers for

        Returns:
            List of registered event handlers
        """
        return self._event_handlers.get(event, [])

    def get_command_handler(
        self,
        command: Type[commands.Command],
    ) -> Optional[Union[Callable, CommandHandlerABC]]:
        """
        Get registered handler for a specific command type.

        Args:
            command: The command type to get handler for

        Returns:
            Registered command handler or None if not found
        """
        return self._command_handlers.get(command, None)

    def set_command_handler(
            self,
            cmd: Type[commands.Command],
            handler: Union[Callable, CommandHandlerABC],
    ) -> None:
        """
        Register a handler for a specific command type.

        Args:
            cmd: The command type to register handler for
            handler: Command handler (callable or CommandHandlerABC instance)
        """
        self._command_handlers[cmd] = handler

    def handle(self, message: Message, *args, **kwargs) -> List:
        """
        Handle a message (command or event) synchronously.

        Args:
            message: The message to handle
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            List of results from handlers
        """
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
        """
        Internal method to handle events.

        Args:
            event: The event to handle
            queue: Queue for handling messages in sequence
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            List of results from event handlers
        """
        results = []

        try:
            handlers = self._event_handlers[type(event)]
        except KeyError:
            logger.error(f"Event handlers for {type(event)} does not exist")
            return results

        for handler in handlers:
            logger.debug(f"Handling event {event} with handler {handler}")

            try:
                if isinstance(handler, EventHandlerABC):
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
        """
        Internal method to handle commands.

        Args:
            cmd: The command to handle
            queue: Queue for handling messages in sequence
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            Result from command handler
        """
        logger.debug(f"Handling command {cmd}")

        try:
            handler = self._command_handlers.get(type(cmd))

            assert handler, f"Handler for {type(cmd)} not found"

            if isinstance(handler, CommandHandlerABC):
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


class AsyncMessageBus(AsyncMessageBusABC):
    """
    Concrete implementation of the asynchronous message bus.

    This class provides a complete implementation for handling commands and events
    in an asynchronous manner, with support for outbox pattern integration.

    Example:
        ```python
        from message_bus import AsyncMessageBus

        # Create and configure async message bus
        async_message_bus = AsyncMessageBus(
            event_handlers={MyEvent: [MyAsyncEventHandler()]},
            command_handlers={MyCommand: MyAsyncCommandHandler()}
        )

        # Handle messages asynchronously
        result = await async_message_bus.handle(MyCommand(data="test"))
        ```
    """

    def __init__(
            self,
            event_handlers: Optional[Dict[Type[events.Event], List[Union[Callable, EventHandlerABC]]]] = None,
            command_handlers: Optional[Dict[Type[commands.Command], Union[Callable, CommandHandlerABC]]] = None,
    ):
        """
        Initialize the async message bus with optional pre-configured handlers.

        Args:
            event_handlers: Optional mapping of event types to their handlers
            command_handlers: Optional mapping of command types to their handlers
        """
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
    ) -> None:
        """
        Register event handlers for a specific event type.

        Args:
            event: The event type to register handlers for
            handlers: List of event handlers (callable or EventHandlerABC instances)
        """
        self._event_handlers[event] = handlers

    def set_command_handler(
            self,
            cmd: Type[commands.Command],
            handler: Union[Callable, CommandHandlerABC],
    ) -> None:
        """
        Register a handler for a specific command type.

        Args:
            cmd: The command type to register handler for
            handler: Command handler (callable or CommandHandlerABC instance)
        """
        self._command_handlers[cmd] = handler

    def get_event_handlers(
            self,
            event: Type[events.Event],
    ) -> List[Union[Callable, EventHandlerABC]]:
        """
        Get registered event handlers for a specific event type.

        Args:
            event: The event type to get handlers for

        Returns:
            List of registered event handlers
        """
        return self._event_handlers.get(event, [])

    def get_command_handler(
            self,
            command: Type[commands.Command],
    ) -> Optional[Union[Callable, CommandHandlerABC]]:
        """
        Get registered handler for a specific command type.

        Args:
            command: The command type to get handler for

        Returns:
            Registered command handler or None if not found
        """
        return self._command_handlers.get(command)

    async def process_outbox(self, outbox_repo: AsyncOutBoxRepoABC) -> None:
        """
        Process all unprocessed messages from the outbox repository asynchronously.

        This method should be called periodically to process messages that have been
        persisted to the outbox but not yet processed.

        Args:
            outbox_repo: Repository containing outbox messages to process
        """
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

    async def batch_handle(self, messages: List[Message], *args, **kwargs) -> List:
        """
        Handle multiple messages in batch asynchronously.

        Args:
            messages: List of messages to handle
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            List of results from handling all messages
        """
        max_concurrency = kwargs.pop("max_concurrency", 5)
        q: asyncio.Queue[Tuple[int, Any]] = asyncio.Queue()
        results: List[Any] = [None] * len(messages)

        for idx, msg in enumerate(messages):
            q.put_nowait((idx, msg))

        async def worker():
            while True:
                try:
                    idx, msg = await q.get()
                except asyncio.CancelledError:
                    break
                try:
                    res = await self.handle(msg, *args, **kwargs)
                    results[idx] = res
                except Exception as e:
                    results[idx] = e
                finally:
                    q.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(max_concurrency)]
        try:
            await q.join()
        finally:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        for msg, res in zip(messages, results):
            if isinstance(res, Exception):
                logger.error("Error processing message %r: %s", msg, res)

        return [r for r in results if not isinstance(r, Exception)]

    async def handle(self, message: Message, *args, **kwargs) -> List:
        """
        Handle a message (command or event) asynchronously.

        Args:
            message: The message to handle
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            List of results from handlers
        """
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
        """
        Internal method to handle events asynchronously.

        Args:
            event: The event to handle
            queue: Queue for handling messages in sequence
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            Tuple of results from event handlers
        """
        coroutines = []

        try:
            handlers = self._event_handlers[type(event)]
        except KeyError:
            logger.error(f"Event handlers for {type(event)} does not exist")
            return tuple()

        for handler in handlers:
            logger.debug(f"Handling event {event} with handler {handler}")

            try:
                if isinstance(handler, EventHandlerABC):
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
        """
        Internal method to handle commands.

        Args:
            cmd: The command to handle
            queue: Queue for handling messages in sequence
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            Result from command handler
        """
        logger.debug(f"Handling command {cmd}")

        try:
            handler = self._command_handlers[type(cmd)]

            if isinstance(handler, CommandHandlerABC):
                result = handler.handle(cmd, context=self.context, *args, **kwargs)
                queue.extend(handler.emitted_messages)
            else:
                result = handler(cmd, context=self.context, *args, **kwargs)
        except Exception as e:
            logger.exception(f"Error handling command {cmd}", exc_info=e)
            raise

        return result
