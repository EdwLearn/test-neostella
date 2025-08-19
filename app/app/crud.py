from datetime import datetime
from typing import Optional

from sqlalchemy import func, desc
from sqlalchemy.orm import Session

from .db.models import Order

def get_orders_summary(
    db: Session,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    customer_id: Optional[str] = None,
    order_by: str = 'totaml_amount',
    limit: int = 50
    ):
    
    q = db.query(Order)
    
    if start_date:
        q = q.filter(Order.created_at >= start_date)
    if end_date:
        q = q.filter(Order.created_at < end_date)
    if customer_id:
        q = q.filter(Order.customer_id == customer_id)
        
        
    subq = (
    q.with_entities(
        Order.order_id.label("order_id"),
        Order.customer_id.label("customer_id"),
        Order.amount.label("amount"),
        Order.created_at.label("created_at"),
        )
    ).subquery()
    
    
    total_orders = db.query(func.count(subq.c.order_id)).scalar()
    total_amount = db.query(func.coalesce(func.sum(subq.c.amount), 0)).scalar()
    
    
    
    by_customer_q = (
        db.query(
            subq.c.customer_id,
            func.count(subq.c.order_id).label("orders"),
            func.coalesce(func.sum(subq.c.amount), 0).label("total_amount")
        )
        .select_from(subq)
        .group_by(subq.c.customer_id)
    )
    
    by_customer_q = by_customer_q.order_by(
        desc("orders") if order_by == "orders" else desc("total_amount")
    )
    
    rows = by_customer_q.limit(limit).all()
    
    return {
        "total_orders": int(total_orders or 0),
        "total_amount": float(total_amount or 0),
        "by_customer": [
            {
                "customer_id": cid,
                "orders": int(orders),
                "total_amount": float(amount)
            }
            for cid, orders, amount in rows
        ]
    }