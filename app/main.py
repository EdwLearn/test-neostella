from logging import exception
from unittest import result


from sqlalchemy.sql.operators import ge
from fastapi import FastAPI, Query, UploadFile, File, HTTPException
from celery.result import AsyncResult
from pydantic import BaseModel
from typing import Optional, Any, List
from datetime import datetime
from io import StringIO
import csv



from .tasks import add_async
from .db.session import SessionLocal, engine, Base
from .db import models
from .celery_app import celery
from .schemas import OrderIn
from .tasks import add_async, import_orders_async
from .crud import get_orders_summary

class TaskStatus(BaseModel):
    task_id: str
    state: str
    ready: bool
    result: Optional[object] = None
    error: Optional[str] = None

app = FastAPI(title= 'Neostella Tech Test - API')

@app.on_event("startup")
def on_statup():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally: 
        db.close()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/task/add")
def enqueue_add(a: int, b: int):
    res = add_async.delay(a,b)
    return {"task_id": res.id}

@app.get("/task/{task_id}", response_model = TaskStatus)
def get_task(task_id: str):
    #Consulta de el estado de una tarea de Celery
    
    async_result = AsyncResult(task_id, app=celery)
    
    payload = {
        "task_id": task_id,
        "state": async_result.state,
        "ready": async_result.ready(),
        "result": None ,
        "error": None
    }
    
    if async_result.successful():
        payload["result"] = async_result.result
    elif async_result.failed():
        payload["error"] = str(async_result.result)
        
    return payload

@app.post("/orders/import")
def import_orders(payload: List[OrderIn]):
    rows = [p.model_dump(mode='json') for p in payload] #Convierte modelo a JSON
    res = import_orders_async.delay(rows)
    return {"task_id": res.id}

@app.post("/orders/import-csv")
async def import_orders_csv(file: UploadFile = File(...)):
    # Validate CSV file
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code = 400, detail="Accept only .csv files")
    
    # Read like text
    content = await file.read()
    try:
        text = content.decode("utf-8")
        
    except UnicodeDecodeError:
        raise HTTPException(status_code = 400, detail= "File not in UTF-8")
    
    #Parse to Dict
    reader = csv.DictReader(StringIO(text))
    rows = []
    for idx, row in enumerate(reader,start=1):
        try:
            #Validate with Pydantic
            order = OrderIn(
                order_id=row["order_id"],
                customer_id = row["customer_id"],
                created_at= row["created_at"],
            )
            rows.append(order.model_dump(mode='json'))
        except Exception as e:
            raise HTTPException(
                status_code = 400,
                detail=f'Error in row {idx} {e}'
            )
            
    if not rows:
        raise HTTPException(status_code = 400, detail= "Empty or invalid CSV file")
    
    res = import_orders_async.delay(rows)
    return {"task_id". res.id}


@app.get("/orders/summary")
def orders_summary(
    start_date: Optional[datetime] = Query(None, description="ISO-8601, e.g. 2025-08-13T00:00:00Z"),
    end_date: Optional[datetime]   = Query(None, description="ISO-8601"),
    customer_id: Optional[str]     = None,
    order_by: str                  = Query("total_amout", pattern="^(total_amount|orders)$"),
    limit: int                     = Query(50, ge=1, le=1000),
):
    db = SessionLocal()
    try:
        return get_orders_summary(db, start_date, end_date, customer_id, order_by, limit)
    finally:
        db.close()
        
        
    