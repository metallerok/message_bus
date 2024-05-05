# OutBox

```python
from message_bus.repositories.outbox import OutBoxRepoABC

class OutBox(Base):
    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=lambda: str(uuid.uuid4()))

    type: Mapped[str] = mapped_column(nullable=False)
    message_type: Mapped[str] = mapped_column(nullable=False)
    message: Mapped['Message'] = mapped_column(type_=SAMessage, nullable=False)

    datetime: Mapped[dt.datetime] = mapped_column(default=dt.datetime.utcnow)
    processed: Mapped[Optional[dt.datetime]] = mapped_column(default=None)

class SAOutBoxRepo(OutBoxRepoABC):
    def __init__(self, db_session: AsyncSession) -> None:
        self._db_session = db_session

        super().__init__()

    def get_model(self) -> Type:
        return OutBox

    def _add(self, outbox_message: OutBox):
        self._db_session.add(outbox_message)

    async def list_unprocessed(self) -> List:
        query = sa.select(
            OutBox
        ).with_for_update()

        result = await self._db_session.execute(query)

    return list(result.scalars().all())

class ProcessOrder(Command):
    order_id: int

outbox_repo = SAOutBoxRepo(db_session)
message_bus = AsyncMessageBus()

order = create_order()

message_bus.register_outbox_message(
    outbox_repo,
    message=ProcessOrder(order_id=order.id)
)

# save order ans outbox message with same transation
db_session.commit()
```

Your model must have these fields
```python
@dataclass
class ExampleOutboxModel:
    id: uuid.UUID
    type: str
    message_type: str
    message: Message
```
