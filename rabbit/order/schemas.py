from pydantic import BaseModel

class OrderBase(BaseModel):
    product_id: int
    owner_id: int
    quantity: int

class OrderCreate(OrderBase):
    pass

class OrderResponse(OrderBase):
    id: int

    class Config:
        orm_mode = True
