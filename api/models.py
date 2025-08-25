from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class StockPrice(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

    @field_validator("date", mode="before")
    @classmethod
    def convert_datetime_to_date(cls, v: Any) -> date:
        if isinstance(v, datetime):
            return v.date()
        return v