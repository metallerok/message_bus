from typing import Union
from message_bus import events
from message_bus import commands

Message = Union[events.Event, commands.Command]
