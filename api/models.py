from datetime import date, datetime
from typing import Any, Optional, List

from pydantic import BaseModel, validator


class SymbolsMaster(BaseModel):
    id: str
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    type: Optional[str] = None
    currency: Optional[str] = None
    status: Optional[str] = None
    source: Optional[str] = None
    failedAttempts: Optional[int] = 0
    lastUpdated: datetime

    class Config:
        orm_mode = True


class SymbolsMeta(BaseModel):
    id: str
    symbol: str
    sector: Optional[str] = None
    industry: Optional[str] = None
    marketCap: Optional[float] = None
    dividendYield: Optional[float] = None
    debtEq: Optional[float] = None
    rOE: Optional[float] = None
    website: Optional[str] = None
    country: Optional[str] = None
    description: Optional[str] = None
    logo: Optional[str] = None
    source: Optional[str] = None
    lastUpdated: datetime

    class Config:
        orm_mode = True


class SymbolQuote(BaseModel):
    id: str
    symbol: str
    price: Optional[float] = None
    change: Optional[float] = None
    volume: Optional[int] = None
    pE: Optional[float] = None
    pEG: Optional[float] = None
    pB: Optional[float] = None
    pFCF: Optional[float] = None
    ePSthisY: Optional[float] = None
    ePSpast5Y: Optional[float] = None
    beta: Optional[float] = None
    w52High: Optional[float] = None
    w52Low: Optional[float] = None
    recommendation: Optional[str] = None
    lastUpdated: datetime

    class Config:
        orm_mode = True


class StockPrice(BaseModel):
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

    @validator("date", pre=True)
    @classmethod
    def convert_datetime_to_date(cls, v: Any) -> date:
        if isinstance(v, datetime):
            return v.date()
        return v

    class Config:
        orm_mode = True


class UpdatedSymbolData(BaseModel):
    meta: Optional[SymbolsMeta] = None
    quote: Optional[SymbolQuote] = None
    price_history: List[StockPrice] = []