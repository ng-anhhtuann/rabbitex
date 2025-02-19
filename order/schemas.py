from pydantic import BaseModel

class OrderBase(BaseModel):
    product_name: str
    quantity: int

class OrderCreate(OrderBase):
    pass

class OrderResponse(OrderBase):
    id: int

    class Config:
        orm_mode = True
