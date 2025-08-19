import decimal
from .db.session import Base
from pydantic import BaseModel, Field, condecimal
from datetime import datetime

class OrderIn(BaseModel):
    order_id: str = Field(min_length =1)
    customer_id: str = Field(min_length = 1)
    amount: condecimal(max_digits = 10, decimal_places = 2)
    created_at: datetime
    