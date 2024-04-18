from typing import Union
import events
import commands

Message = Union[events.Event, commands.Command]
