import datetime as dt
import uuid
from dataclasses_serialization.json import JSONSerializer


serializer = JSONSerializer
serializer.serialization_functions[uuid.UUID] = lambda uuid_: str(uuid_)
serializer.deserialization_functions[uuid.UUID] = lambda _, uuid_: uuid.UUID(uuid_)

serializer.serialization_functions[dt.date] = lambda x: x.isoformat()
serializer.deserialization_functions[dt.date] = lambda _, x: dt.date.fromisoformat(x)

serializer.serialization_functions[dt.datetime] = lambda x: x.isoformat()
serializer.deserialization_functions[dt.datetime] = lambda _, x: dt.datetime.fromisoformat(x)


class Message:
    def serialize(self) -> dict:
        return serializer.serialize(self)

    @classmethod
    def deserialize(cls, data: dict) -> 'Message':
        return serializer.deserialize(cls, data)
