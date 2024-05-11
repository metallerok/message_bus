import uuid
from dataclasses_serialization.json import JSONSerializer


serializer = JSONSerializer
serializer.serialization_functions[uuid.UUID] = lambda uuid_: str(uuid_)
serializer.deserialization_functions[uuid.UUID] = lambda _, uuid_: uuid.UUID(uuid_)


class Message:
    def serialize(self) -> dict:
        return serializer.serialize(self)

    @classmethod
    def deserialize(cls, data: dict) -> 'Message':
        return serializer.deserialize(cls, data)
