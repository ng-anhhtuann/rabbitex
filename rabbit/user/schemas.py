from pydantic import BaseModel

class UserBase(BaseModel):
    name: str
    balance: int

class UserCreate(UserBase):
    pass

class UserUpdate(UserBase):
    pass

class UserResponse(UserBase):
    id: int

    class Config:
        orm_mode = True
