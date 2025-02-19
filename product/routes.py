from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import schemas, crud, database

router = APIRouter()

@router.get("/products", response_model=list[schemas.ProductResponse])
def read_products(db: Session = Depends(database.get_db), skip: int = 0, limit: int = 10):
    return crud.get_products(db, skip, limit)

@router.get("/products/{product_id}", response_model=schemas.ProductResponse)
def read_product(product_id: int, db: Session = Depends(database.get_db)):
    db_product = crud.get_product(db, product_id)
    if db_product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return db_product

@router.post("/products", response_model=schemas.ProductResponse)
def create_product(product: schemas.ProductCreate, db: Session = Depends(database.get_db)):
    return crud.create_product(db, product)

@router.put("/products/{product_id}", response_model=schemas.ProductResponse)
def update_product(product_id: int, product: schemas.ProductUpdate, db: Session = Depends(database.get_db)):
    db_product = crud.update_product(db, product_id, product)
    if db_product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return db_product

@router.delete("/products/{product_id}")
def delete_product(product_id: int, db: Session = Depends(database.get_db)):
    db_product = crud.delete_product(db, product_id)
    if db_product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"message": "Product deleted successfully"}
