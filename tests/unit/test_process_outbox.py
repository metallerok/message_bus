import asyncio

from src.message_bus.core import AsyncMessageBus, MessageBus


class FakeOutBoxRow:
    """Emulates ORM expiry semantics of a real outbox row.

    Session.rollback() expires every loaded instance; touching any attribute
    of an expired instance afterwards performs a refresh (a sync IO that
    breaks async sessions with MissingGreenlet).
    """

    def __init__(self, row_id):
        self._id = row_id
        self.processed = False
        self.expired = False

    @property
    def id(self):
        if self.expired:
            raise AssertionError("outbox row attribute accessed after expire")
        return self._id


def _row(row_id):
    return FakeOutBoxRow(row_id)


class FakeAsyncOutBoxRepo:
    """Emulates SELECT ... FOR UPDATE SKIP LOCKED with per-transaction locks.

    Each repo instance represents a separate DB session/transaction (like two
    concurrent process_outbox callers). A row is claimed (locked) when returned
    by list_unprocessed and stays locked until this repo's save()/rollback().
    Other repos see the row as locked and skip it.
    """

    def __init__(self, rows, peers=None):
        self.rows = rows
        self.locked = set()
        self.save_calls = 0
        self.peers = peers if peers is not None else []

    def _locked_by_others(self):
        locked = set()
        for repo in self.peers:
            locked.update(repo.locked)
        return locked

    async def list_unprocessed(self, limit=None, exclude_ids=None):
        exclude = set(exclude_ids or [])
        busy = self.locked | self._locked_by_others()
        available = [
            row for row in self.rows
            if not row.processed and row._id not in exclude and row._id not in busy
        ]
        claimed = available[:limit] if limit else available

        for row in claimed:
            row.expired = False
        self.locked.update(row._id for row in claimed)

        return claimed

    async def save(self):
        self.save_calls += 1
        self.locked.clear()

    async def rollback(self):
        self.locked.clear()
        for row in self.rows:
            row.expired = True


class PublishingHandler:
    def __init__(self, fail_ids=frozenset()):
        self.fail_ids = set(fail_ids)
        self.published = []

    async def handle(self, outbox_message, context):
        # Yield control so concurrent publishers interleave like in production.
        await asyncio.sleep(0)
        if outbox_message.id in self.fail_ids:
            raise RuntimeError("publish failed")
        outbox_message.processed = True
        self.published.append(outbox_message.id)


class AsyncBus(AsyncMessageBus):
    def __init__(self, handlers):
        self._outbox_handlers = handlers


def _wire_repos(repo_a, repo_b):
    registry = []
    for row in repo_a.rows:
        row._repos = registry
    registry.extend([repo_a, repo_b])
    return registry


def test_process_outbox_publishes_each_row_once_under_concurrency():
    """Reproduces the production race.

    Publisher A commits after its first publish: the commit marks row 0 and
    releases the row locks of all remaining rows. Publisher B starts exactly
    at that moment and fetches the not-yet-published rows. The batch-fatching
    implementation then publishes them a second time from its stale in-memory
    list; the one-row-at-a-time implementation must not.
    """
    rows = [_row(i) for i in range(20)]
    repo_a = FakeAsyncOutBoxRepo(rows)
    repo_b = FakeAsyncOutBoxRepo(rows, peers=[repo_a])
    repo_a.peers.append(repo_b)

    first_a_save = asyncio.Event()
    original_save = repo_a.save

    async def save_notifying_first_commit():
        was_first = repo_a.save_calls == 0
        await original_save()
        if was_first:
            first_a_save.set()

    repo_a.save = save_notifying_first_commit

    handler_a = PublishingHandler()
    handler_b = PublishingHandler()

    async def publisher_a():
        await AsyncBus([handler_a]).process_outbox(repo_a)

    async def publisher_b():
        await first_a_save.wait()
        await AsyncBus([handler_b]).process_outbox(repo_b)

    async def scenario():
        await asyncio.gather(publisher_a(), publisher_b())

    asyncio.run(scenario())

    published_ids = handler_a.published + handler_b.published
    assert sorted(published_ids) == list(range(20))
    # A row must never be published twice.
    assert len(published_ids) == len(set(published_ids))
    assert all(row.processed for row in rows)


def test_process_outbox_failed_row_does_not_block_the_batch():
    rows = [_row(i) for i in range(5)]
    repo = FakeAsyncOutBoxRepo(rows)

    handler = PublishingHandler(fail_ids={2})

    async def scenario():
        await AsyncBus([handler]).process_outbox(repo)

    asyncio.run(scenario())

    assert sorted(handler.published) == [0, 1, 3, 4]
    assert [row._id for row in rows if not row.processed] == [2]

    # The failed row is retried on the next run once the handler recovers.
    handler.fail_ids.clear()
    asyncio.run(scenario())
    assert sorted(handler.published) == [0, 1, 2, 3, 4]


def test_process_outbox_empty_outbox_is_noop():
    repo = FakeAsyncOutBoxRepo([])
    asyncio.run(AsyncBus([PublishingHandler()]).process_outbox(repo))
    assert repo.save_calls == 0


def test_sync_process_outbox_publishes_all_rows():
    rows = [_row(i) for i in range(3)]

    class FakeSyncRepo:
        def __init__(self):
            self.rows = rows

        def list_unprocessed(self, limit=None, exclude_ids=None):
            exclude = set(exclude_ids or [])
            available = [row for row in rows if not row.processed and row.id not in exclude]
            return available[:limit] if limit else available

        def save(self):
            pass

        def rollback(self):
            pass

    published = []

    class SyncHandler:
        def handle(self, outbox_message, context):
            if outbox_message.id == 1:
                raise RuntimeError("publish failed")
            outbox_message.processed = True
            published.append(outbox_message.id)

    class SyncBus(MessageBus):
        def __init__(self, handlers):
            self._outbox_handlers = handlers

    SyncBus([SyncHandler()]).process_outbox(FakeSyncRepo())

    assert published == [0, 2]
    assert [row.id for row in rows if not row.processed] == [1]
