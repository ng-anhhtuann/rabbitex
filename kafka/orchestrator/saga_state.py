from enum import Enum
import uuid
import time
from datetime import datetime

class SagaStatus(str, Enum):
    STARTED = "STARTED"
    ORDER_CREATED = "ORDER_CREATED"
    PRODUCT_RESERVED = "PRODUCT_RESERVED"
    PAYMENT_PROCESSED = "PAYMENT_PROCESSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"

class SagaStep:
    def __init__(self, name, status, data=None, timestamp=None):
        self.name = name
        self.status = status
        self.data = data or {}
        self.timestamp = timestamp or datetime.now().isoformat()

class SagaState:
    def __init__(self, saga_id=None, order_data=None):
        self.saga_id = saga_id or str(uuid.uuid4())
        self.status = SagaStatus.STARTED
        self.steps = []
        self.order_data = order_data or {}
        self.start_time = time.time()
        self.end_time = None
        
    def add_step(self, step_name, status, data=None):
        """Add a new step to the saga"""
        step = SagaStep(step_name, status, data)
        self.steps.append(step)
        return step
        
    def update_status(self, status):
        """Update the saga status"""
        self.status = status
        if status in [SagaStatus.COMPLETED, SagaStatus.FAILED, SagaStatus.COMPENSATED]:
            self.end_time = time.time()
            
    def to_dict(self):
        """Convert saga state to dictionary"""
        return {
            "saga_id": self.saga_id,
            "status": self.status,
            "order_data": self.order_data,
            "steps": [
                {
                    "name": step.name,
                    "status": step.status,
                    "data": step.data,
                    "timestamp": step.timestamp
                } for step in self.steps
            ],
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": (self.end_time - self.start_time) if self.end_time else None
        }

saga_store = {} 