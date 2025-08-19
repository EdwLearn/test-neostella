from typing import List, Dict
from decimal import Decimal
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from datetime import datetime

from .celery_app import celery
from .db.session import SessionLocal
from .db.models import Order, ImportError


def _parse_created_at(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        v = value.replace("Z", "+00:00")
        return datetime.fromisoformat(v)
    raise ValueError(f'created_at invalido: {value!r}')

@celery.task(name = "task.import_orders")
def import_orders_async(rows: List[Dict]):
    db: Session = SessionLocal()
    inserted = 0
    errors = 0
    
    try:
        for idx, r in enumerate(rows, start=1):
            try:
                if not r.get("order_id"):
                    raise ValueError("Falta 'order_id'")
                if not r.get("customer_id"):
                    raise ValueError("Falta 'customer_id'")
                if r.get("amount") is None:
                    raise ValueError("Falta 'amount'")
                if r.get("created_at") is None:
                    raise ValueError("Falta 'created_at'")
                
                order = Order(
                    order_id=str(r["order_id"]),
                    customer_id=str(r["customer_id"]),
                    amount=Decimal(str(r["amount"])),
                    created_at=r["created_at"],
                )
                db.add(order)
                db.flush()
                inserted += 1
                
                
            except IntegrityError as ie:
                db.rollback()
                errors += 1
                db.add(ImportError(
                    task_id=import_orders_async.request.id,
                    row_number=idx,
                    error_message=f'IntegrityError (posible duplicado order_id): {ie.orig}'
                ))
                db.flush()
                
        db.commit()
        return {"inserted": inserted, "errors": errors}
        
    finally:
        db.close()
        
@celery.task(name="app.add_async")
def add_async(a: int, b: int) -> int:
    return a + b