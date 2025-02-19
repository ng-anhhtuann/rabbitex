from pydantic import BaseModel

class ProductBase(BaseModel):
    name: str
    price: float
    stock: int

class ProductCreate(ProductBase):
    pass

class ProductUpdate(ProductBase):
    pass

class ProductResponse(ProductBase):
    id: int

    class Config:
        orm_mode = True
