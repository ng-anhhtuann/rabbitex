from sqlalchemy import Column, Integer, String, Float, DateTime, JSON
from sqlalchemy.sql import func
from database import Base

class Saga(Base):
    __tablename__ = "sagas"

    id = Column(Integer, primary_key=True, index=True)
    saga_id = Column(String, unique=True, index=True)
    order_id = Column(Integer)
    status = Column(String)
    current_step = Column(String)
    step_history = Column(JSON)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    def to_dict(self):
        return {
            "id": self.id,
            "saga_id": self.saga_id,
            "order_id": self.order_id,
            "status": self.status,
            "current_step": self.current_step,
            "step_history": self.step_history,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        } 