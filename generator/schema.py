#used to build a schema json for one message shape for the 
#generator publishes to kafka and spark job will read 
#creates a new transaction id and key

from __future__ import annotations
import json 
from dataclasses import asdict,dataclass 

from decimal import Decimal
from typing import Any
from datetime import datetime

def format_event_time(moment: datetime) -> str:
    """Render an event time as ISO-8601 with millisecond precision."""
    return moment.isoformat(timespec="milliseconds")


PAYSIM_ID_PREFIX = "PS"  #this will represent the already data in paysim
INJECTED_ID_PREFIX = "INJ" # this will represent the injected data for tests scripts and audit logs

def build_transaction_id (prefix:str,sequence:int) -> str:
    return f"{prefix}-{sequence:09d}"

@dataclass(frozen=True)
class Transaction:
    transaction_id : str
    event_time: str
    transaction_type: str
    amount: float
    name_orig: str
    old_balance_orig: float
    new_balance_orig: float
    name_dest: str
    old_balance_dest: float
    new_balance_dest: float
    latitude: float
    longitude: float

    def message_key(self) -> bytes:
        return self.name_orig.encode("utf-8")

    def message_value(self) -> bytes:
        raw_dict= asdict(self) #used to convert the Transaction into standard python dict
        monetary_feilds= {
            "amount",
            "old_balance_orig",
            "new_balance_orig",
            "old_balance_dest",
            "new_balance_dest",
        }

        for field in monetary_feilds:
            raw_dict[field] = float(round(Decimal(str(raw_dict[field])),2))

        return json.dumps(raw_dict,separators=(",",":")).encode("utf-8")
    @classmethod
    def from_json(cls,payload:bytes | str) -> Transaction:
        if isinstance(payload,bytes):
            payload= payload.decode("utf-8")

        data: dict[str,Any]= json.loads(payload)
        return cls(**data)