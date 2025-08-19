import datetime
from sqlalchemy import Column, String, Integer, Numeric, DateTime
from sqlalchemy.sql import func
from .session import Base

class Order(Base):
    __tablename__ = "orders"
    
    order_id = Column(String, primary_key=True)
    customer_id = Column(String, nullable = False, index = True)
    amount = Column(Numeric(10,2), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    ingested_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    
class ImportError(Base):
    __tablename__ = "import_errors"
    
    id = Column(Integer, primary_key= True, autoincrement=True)
    task_id = Column(String, index= True, nullable= False)
    row_number = Column(Integer, nullable=False)
    error_message = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    