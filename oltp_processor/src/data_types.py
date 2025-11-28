import datetime as dt
import decimal
import uuid
from dataclasses import dataclass


@dataclass
class DatasetRow:
    event_time: dt.datetime
    event_type: str
    product_id: int
    category_id: str
    category_code: str | None
    brand: str
    price: decimal.Decimal
    user_id: int
    user_session: uuid.UUID
    row_id: uuid.UUID

    @classmethod
    def from_dict(cls, data):
        casts = {
            "event_time": dt.datetime.fromisoformat,
            "price": decimal.Decimal,
            "user_session": uuid.UUID,
            "row_id": uuid.UUID,
        }
        return cls(**{k: casts[k](v) if k in casts else v for (k, v) in data.items()})
