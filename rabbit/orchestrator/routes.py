from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import schemas, crud, database

router = APIRouter()

@router.get("/sagas", response_model=list[schemas.SagaResponse])
def read_sagas(db: Session = Depends(database.get_db), skip: int = 0, limit: int = 10):
    return crud.get_sagas(db, skip, limit)

@router.get("/sagas/{saga_id}", response_model=schemas.SagaResponse)
def read_saga(saga_id: str, db: Session = Depends(database.get_db)):
    db_saga = crud.get_saga(db, saga_id)
    if db_saga is None:
        raise HTTPException(status_code=404, detail="Saga not found")
    return db_saga

@router.get("/sagas/order/{order_id}", response_model=schemas.SagaResponse)
def read_saga_by_order(order_id: int, db: Session = Depends(database.get_db)):
    db_saga = crud.get_saga_by_order_id(db, order_id)
    if db_saga is None:
        raise HTTPException(status_code=404, detail="Saga not found for this order")
    return db_saga

@router.post("/orders", response_model=dict)
def create_order(order_data: schemas.OrderRequest, db: Session = Depends(database.get_db)):

    order_id = hash(f"{order_data.product_id}:{order_data.owner_id}:{order_data.quantity}") % 10000
    
    existing_saga = crud.get_saga_by_order_id(db, order_id)
    if existing_saga:
        raise HTTPException(status_code=400, detail=f"A saga already exists for order {order_id}")
    
    # Start the saga
    saga = crud.start_order_saga(db, order_data, order_id)
    
    return {
        "message": "Order saga started successfully",
        "saga_id": saga["saga_id"],
        "order_id": saga["order_id"],
        "status": saga["status"]
    } 