from dataclasses import dataclass
from src.message_bus.events import Event


def test_message_default_fields():
    @dataclass
    class TestEvent(Event):
        payload: str

    event = TestEvent(payload="test")
    assert event.payload == "test"
    assert event.serialize() == {"payload": "test"}
