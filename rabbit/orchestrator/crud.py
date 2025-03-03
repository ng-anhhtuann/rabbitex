from sqlalchemy.orm import Session
import models, schemas
from rabbitmq import publish_saga_command
import uuid
import json

def get_sagas(db: Session, skip: int = 0, limit: int = 10):
    return db.query(models.Saga).offset(skip).limit(limit).all()

def get_saga(db: Session, saga_id: str):
    return db.query(models.Saga).filter(models.Saga.saga_id == saga_id).first()

def get_saga_by_order_id(db: Session, order_id: int):
    return db.query(models.Saga).filter(models.Saga.order_id == order_id).first()

def create_saga(db: Session, saga: schemas.SagaCreate):
    saga_id = str(uuid.uuid4())
    db_saga = models.Saga(
        saga_id=saga_id,
        order_id=saga.order_id,
        status="STARTED",
        current_step="INIT",
        step_history={"steps": []}
    )
    db.add(db_saga)
    db.commit()
    db.refresh(db_saga)
    return db_saga

def update_saga_status(db: Session, saga_id: str, status: str, current_step: str, step_result=None):
    db_saga = db.query(models.Saga).filter(models.Saga.saga_id == saga_id).first()
    if db_saga:
        db_saga.status = status
        db_saga.current_step = current_step
        
        # Update step history
        step_history = db_saga.step_history or {"steps": []}
        step_history["steps"].append({
            "step": current_step,
            "status": status,
            "timestamp": str(uuid.uuid1()),
            "result": step_result
        })
        db_saga.step_history = step_history
        
        db.commit()
        db.refresh(db_saga)
    return db_saga

def start_order_saga(db: Session, order_data: schemas.OrderRequest, order_id: int):
    """
    Start a new saga for order processing
    
    The saga follows these steps:
    1. Check product stock
    2. Check user balance
    3. Update user balance (deduct payment)
    4. Update product stock
    5. Update order status
    
    If any step fails, compensating transactions are triggered
    """
    # Create a new saga record
    saga = create_saga(db, schemas.SagaCreate(order_id=order_id))
    
    # Start the first step: check product stock
    update_saga_status(db, saga.saga_id, "PROCESSING", "CHECK_STOCK")
    
    # Send command to product service to check stock
    publish_saga_command(
        saga.saga_id,
        "product",
        "check_stock",
        {
            "product_id": order_data.product_id,
            "quantity": order_data.quantity,
            "order_id": order_id
        }
    )
    
    return saga.to_dict()

def handle_check_stock_success(db: Session, saga_id: str, event_data: dict):
    """Handle successful stock check"""
    payload = event_data["payload"]
    update_saga_status(db, saga_id, "PROCESSING", "CHECK_BALANCE", payload)
    
    # Send command to user service to check balance
    publish_saga_command(
        saga_id,
        "user",
        "check_balance",
        {
            "owner_id": payload["payload"]["owner_id"],
            "amount": payload["total_price"],
            "order_id": payload["payload"]["order_id"]
        }
    )

def handle_check_stock_failed(db: Session, saga_id: str, event_data: dict):
    """Handle failed stock check"""
    payload = event_data["payload"]
    update_saga_status(db, saga_id, "FAILED", "CHECK_STOCK", payload)
    
    # Send command to order service to update order status
    publish_saga_command(
        saga_id,
        "order",
        "update_status",
        {
            "order_id": payload["payload"]["order_id"],
            "status": "FAILED",
            "reason": payload.get("reason", "Insufficient stock")
        }
    )

def handle_check_balance_success(db: Session, saga_id: str, event_data: dict):
    """Handle successful balance check"""
    payload = event_data["payload"]
    update_saga_status(db, saga_id, "PROCESSING", "UPDATE_BALANCE", payload)
    
    # Send command to user service to update balance
    publish_saga_command(
        saga_id,
        "user",
        "update_balance",
        {
            "owner_id": payload["payload"]["owner_id"],
            "amount": payload["payload"]["amount"],
            "order_id": payload["payload"]["order_id"]
        }
    )

def handle_check_balance_failed(db: Session, saga_id: str, event_data: dict):
    """Handle failed balance check"""
    payload = event_data["payload"]
    update_saga_status(db, saga_id, "FAILED", "CHECK_BALANCE", payload)
    
    # Send command to order service to update order status
    publish_saga_command(
        saga_id,
        "order",
        "update_status",
        {
            "order_id": payload["payload"]["order_id"],
            "status": "FAILED",
            "reason": payload.get("reason", "Insufficient balance")
        }
    )

def handle_update_balance_success(db: Session, saga_id: str, event_data: dict):
    """Handle successful balance update"""
    payload = event_data["payload"]
    update_saga_status(db, saga_id, "PROCESSING", "UPDATE_STOCK", payload)
    
    # Get saga to retrieve product info
    saga = get_saga(db, saga_id)
    step_history = saga.step_history
    check_stock_step = next((step for step in step_history["steps"] if step["step"] == "CHECK_STOCK"), None)
    
    if check_stock_step and check_stock_step["result"]:
        product_data = check_stock_step["result"]
        
        # Send command to product service to update stock
        publish_saga_command(
            saga_id,
            "product",
            "update_stock",
            {
                "product_id": product_data["payload"]["product_id"],
                "quantity": product_data["payload"]["quantity"],
                "order_id": product_data["payload"]["order_id"]
            }
        )
    else:
        # Something went wrong, we don't have product data
        update_saga_status(db, saga_id, "FAILED", "UPDATE_STOCK", {"error": "Missing product data"})
        
        # Compensating transaction: refund the user
        publish_saga_command(
            saga_id,
            "user",
            "update_balance",
            {
                "owner_id": payload["payload"]["owner_id"],
                "amount": -payload["payload"]["amount"],  # Negative amount for refund
                "order_id": payload["payload"]["order_id"]
            },
            compensating=True
        )
        
        # Update order status
        publish_saga_command(
            saga_id,
            "order",
            "update_status",
            {
                "order_id": payload["payload"]["order_id"],
                "status": "FAILED",
                "reason": "Error retrieving product data"
            }
        )

def handle_update_balance_failed(db: Session, saga_id: str, event_data: dict):
    """Handle failed balance update"""
    payload = event_data["payload"]
    update_saga_status(db, saga_id, "FAILED", "UPDATE_BALANCE", payload)
    
    # Send command to order service to update order status
    publish_saga_command(
        saga_id,
        "order",
        "update_status",
        {
            "order_id": payload["payload"]["order_id"],
            "status": "FAILED",
            "reason": payload.get("reason", "Failed to update balance")
        }
    )

def handle_update_stock_success(db: Session, saga_id: str, event_data: dict):
    """Handle successful stock update"""
    payload = event_data["payload"]
    update_saga_status(db, saga_id, "PROCESSING", "UPDATE_ORDER", payload)
    
    # Send command to order service to update order status
    publish_saga_command(
        saga_id,
        "order",
        "update_status",
        {
            "order_id": payload["payload"]["order_id"],
            "status": "SUCCESS"
        }
    )

def handle_update_stock_failed(db: Session, saga_id: str, event_data: dict):
    """Handle failed stock update"""
    payload = event_data["payload"]
    update_saga_status(db, saga_id, "FAILED", "UPDATE_STOCK", payload)
    
    # Compensating transaction: refund the user
    saga = get_saga(db, saga_id)
    step_history = saga.step_history
    update_balance_step = next((step for step in step_history["steps"] if step["step"] == "UPDATE_BALANCE"), None)
    
    if update_balance_step and update_balance_step["result"]:
        balance_data = update_balance_step["result"]
        
        # Send command to user service to refund
        publish_saga_command(
            saga_id,
            "user",
            "update_balance",
            {
                "owner_id": balance_data["payload"]["owner_id"],
                "amount": -balance_data["payload"]["amount"],  # Negative amount for refund
                "order_id": balance_data["payload"]["order_id"]
            },
            compensating=True
        )
    
    # Send command to order service to update order status
    publish_saga_command(
        saga_id,
        "order",
        "update_status",
        {
            "order_id": payload["payload"]["order_id"],
            "status": "FAILED",
            "reason": payload.get("reason", "Failed to update stock")
        }
    )

def handle_update_order_success(db: Session, saga_id: str, event_data: dict):
    """Handle successful order update"""
    payload = event_data["payload"]
    update_saga_status(db, saga_id, "COMPLETED", "DONE", payload)

def handle_update_order_failed(db: Session, saga_id: str, event_data: dict):
    """Handle failed order update"""
    payload = event_data["payload"]
    update_saga_status(db, saga_id, "FAILED", "UPDATE_ORDER", payload)

# Map of event patterns to handler functions
event_handlers = {
    "product.check_stock.success": handle_check_stock_success,
    "product.check_stock.failed": handle_check_stock_failed,
    "user.check_balance.success": handle_check_balance_success,
    "user.check_balance.failed": handle_check_balance_failed,
    "user.update_balance.success": handle_update_balance_success,
    "user.update_balance.failed": handle_update_balance_failed,
    "product.update_stock.success": handle_update_stock_success,
    "product.update_stock.failed": handle_update_stock_failed,
    "order.update_status.success": handle_update_order_success,
    "order.update_status.failed": handle_update_order_failed
} 