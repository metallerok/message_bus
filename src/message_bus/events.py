from dataclasses_serialization.json import JSONSerializer
from uuid import UUID


serializer = JSONSerializer
serializer.serialization_functions[UUID] = lambda uuid_: str(uuid_)
serializer.deserialization_functions[UUID] = lambda cls, uuid_: UUID(uuid_)


class Event:
    def serialize(self) -> dict:
        return serializer.serialize(self)

    @classmethod
    def deserialize(cls, data: dict) -> 'Event':
        return serializer.deserialize(cls, data)
