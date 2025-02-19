from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import schemas, crud, database

router = APIRouter()

@router.get("/orders", response_model=list[schemas.OrderResponse])
def read_orders(db: Session = Depends(database.get_db), skip: int = 0, limit: int = 10):
    return crud.get_orders(db, skip, limit)

@router.get("/orders/{order_id}", response_model=schemas.OrderResponse)
def read_order(order_id: int, db: Session = Depends(database.get_db)):
    db_order = crud.get_order(db, order_id)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return db_order

@router.post("/orders", response_model=schemas.OrderResponse)
def create_order(order: schemas.OrderCreate, db: Session = Depends(database.get_db)):
    return crud.create_order(db, order)

@router.delete("/orders/{order_id}")
def delete_order(order_id: int, db: Session = Depends(database.get_db)):
    db_order = crud.delete_order(db, order_id)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return {"message": "Order deleted successfully"}
