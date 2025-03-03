from pydantic import BaseModel
from typing import Optional, Dict, List, Any
from datetime import datetime

class SagaBase(BaseModel):
    order_id: int
    
class SagaCreate(SagaBase):
    pass

class SagaResponse(SagaBase):
    id: int
    saga_id: str
    status: str
    current_step: str
    step_history: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    class Config:
        orm_mode = True
        
class OrderRequest(BaseModel):
    product_id: int
    owner_id: int
    quantity: int 