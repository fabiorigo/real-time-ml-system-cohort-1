from pydantic import BaseModel

class Ohlc(BaseModel):
    product_id: str
    timestamp_ms: int
    open: float
    high: float
    low: float
    close: float