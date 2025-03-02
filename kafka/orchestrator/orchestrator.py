import threading
from kafka_config import (
    publish_message,
    consume_messages,
    TOPIC_CREATE_ORDER,
    TOPIC_RESERVE_PRODUCT,
    TOPIC_PROCESS_PAYMENT,
    TOPIC_COMPLETE_ORDER,
    TOPIC_CANCEL_ORDER,
    TOPIC_RELEASE_PRODUCT,
    TOPIC_REFUND_PAYMENT,
    TOPIC_ORDER_CREATED,
    TOPIC_PRODUCT_RESERVED,
    TOPIC_PRODUCT_RESERVATION_FAILED,
    TOPIC_PAYMENT_PROCESSED,
    TOPIC_PAYMENT_FAILED,
    TOPIC_ORDER_COMPLETED,
    TOPIC_ORDER_CANCELLED,
    TOPIC_START_ORDER_SAGA,
    TOPIC_CONFIRM_PRODUCT_DEDUCTION,
)
from saga_state import SagaState, SagaStatus, saga_store

class OrderSagaOrchestrator:
    def __init__(self):
        self.command_topics = [
            TOPIC_START_ORDER_SAGA,
        ]
        
        self.event_topics = [
            TOPIC_ORDER_CREATED,
            TOPIC_PRODUCT_RESERVED,
            TOPIC_PRODUCT_RESERVATION_FAILED,
            TOPIC_PAYMENT_PROCESSED,
            TOPIC_PAYMENT_FAILED,
            TOPIC_ORDER_COMPLETED,
            TOPIC_ORDER_CANCELLED,
        ]
        
    def start(self):
        """Start the orchestrator"""
        print("Starting Order Saga Orchestrator...")
        
        # Start command listener
        command_callbacks = {
            TOPIC_START_ORDER_SAGA: self.handle_start_order_saga,
        }
        command_thread = threading.Thread(
            target=consume_messages,
            args=(self.command_topics, command_callbacks),
            daemon=True
        )
        command_thread.start()
        
        # Start event listener
        event_callbacks = {
            TOPIC_ORDER_CREATED: self.handle_order_created,
            TOPIC_PRODUCT_RESERVED: self.handle_product_reserved,
            TOPIC_PRODUCT_RESERVATION_FAILED: self.handle_product_reservation_failed,
            TOPIC_PAYMENT_PROCESSED: self.handle_payment_processed,
            TOPIC_PAYMENT_FAILED: self.handle_payment_failed,
            TOPIC_ORDER_COMPLETED: self.handle_order_completed,
            TOPIC_ORDER_CANCELLED: self.handle_order_cancelled,
        }
        event_thread = threading.Thread(
            target=consume_messages,
            args=(self.event_topics, event_callbacks),
            daemon=True
        )
        event_thread.start()
        
        # Keep threads alive
        command_thread.join()
        event_thread.join()
    
    # Command handlers
    def handle_start_order_saga(self, data):
        """Handle start order saga command from API"""
        print(f"Received start order saga command: {data}")
        
        # Create new saga
        saga_id = data.get("saga_id", None)
        saga = SagaState(saga_id=saga_id, order_data=data)
        saga_store[saga.saga_id] = saga
        
        # Start the saga by sending create order command to Order service
        publish_message(TOPIC_CREATE_ORDER, {
            "saga_id": saga.saga_id,
            "product_id": data.get("product_id"),
            "owner_id": data.get("owner_id"),
            "quantity": data.get("quantity")
        })
        
        return saga.saga_id
    
    # Event handlers
    def handle_order_created(self, data):
        """Handle order created event"""
        print(f"Received order created event: {data}")
        saga_id = data.get("saga_id")
        
        if saga_id not in saga_store:
            print(f"Saga {saga_id} not found")
            return
            
        saga = saga_store[saga_id]
        saga.add_step("ORDER_CREATED", "SUCCESS", data)
        saga.update_status(SagaStatus.ORDER_CREATED)
        saga.order_data["order_id"] = data.get("order_id")
        
        # Next step: Reserve product
        publish_message(TOPIC_RESERVE_PRODUCT, {
            "saga_id": saga_id,
            "order_id": data.get("order_id"),
            "product_id": saga.order_data.get("product_id"),
            "quantity": saga.order_data.get("quantity")
        })
    
    def handle_product_reserved(self, data):
        """Handle product reserved event"""
        print(f"Received product reserved event: {data}")
        saga_id = data.get("saga_id")
        
        if saga_id not in saga_store:
            print(f"Saga {saga_id} not found")
            return
            
        saga = saga_store[saga_id]
        saga.add_step("PRODUCT_RESERVED", "SUCCESS", data)
        saga.update_status(SagaStatus.PRODUCT_RESERVED)
        saga.order_data["amount"] = data.get("amount")
        
        # Next step: Process payment
        publish_message(TOPIC_PROCESS_PAYMENT, {
            "saga_id": saga_id,
            "order_id": saga.order_data.get("order_id"),
            "owner_id": saga.order_data.get("owner_id"),
            "amount": data.get("amount")
        })
    
    def handle_product_reservation_failed(self, data):
        """Handle product reservation failed event"""
        print(f"Received product reservation failed event: {data}")
        saga_id = data.get("saga_id")
        
        if saga_id not in saga_store:
            print(f"Saga {saga_id} not found")
            return
            
        saga = saga_store[saga_id]
        saga.add_step("PRODUCT_RESERVED", "FAILED", data)
        saga.update_status(SagaStatus.FAILED)
        
        # Compensate: Cancel order
        publish_message(TOPIC_CANCEL_ORDER, {
            "saga_id": saga_id,
            "order_id": saga.order_data.get("order_id")
        })
    
    def handle_payment_processed(self, data):
        """Handle payment processed event"""
        print(f"Received payment processed event: {data}")
        saga_id = data.get("saga_id")
        
        if saga_id not in saga_store:
            print(f"Saga {saga_id} not found")
            return
            
        saga = saga_store[saga_id]
        saga.add_step("PAYMENT_PROCESSED", "SUCCESS", data)
        saga.update_status(SagaStatus.PAYMENT_PROCESSED)
        
        # Confirm product deduction before completing order
        publish_message(TOPIC_CONFIRM_PRODUCT_DEDUCTION, {
            "saga_id": saga_id,
            "order_id": saga.order_data.get("order_id"),
            "product_id": saga.order_data.get("product_id"),
            "quantity": saga.order_data.get("quantity")
        })
        
        # Next step: Complete order - will now happen after product stock is deducted
        publish_message(TOPIC_COMPLETE_ORDER, {
            "saga_id": saga_id,
            "order_id": saga.order_data.get("order_id"),
            "status": "SUCCESS"
        })
    
    def handle_payment_failed(self, data):
        """Handle payment failed event"""
        print(f"Received payment failed event: {data}")
        saga_id = data.get("saga_id")
        
        if saga_id not in saga_store:
            print(f"Saga {saga_id} not found")
            return
            
        saga = saga_store[saga_id]
        saga.add_step("PAYMENT_PROCESSED", "FAILED", data)
        saga.update_status(SagaStatus.FAILED)
        
        # Start compensation
        saga.update_status(SagaStatus.COMPENSATING)
        
        # Compensate: Release product
        publish_message(TOPIC_RELEASE_PRODUCT, {
            "saga_id": saga_id,
            "order_id": saga.order_data.get("order_id"),
            "product_id": saga.order_data.get("product_id"),
            "quantity": saga.order_data.get("quantity")
        })
        
        # Compensate: Cancel order
        publish_message(TOPIC_CANCEL_ORDER, {
            "saga_id": saga_id,
            "order_id": saga.order_data.get("order_id")
        })
    
    def handle_order_completed(self, data):
        """Handle order completed event"""
        print(f"Received order completed event: {data}")
        saga_id = data.get("saga_id")
        
        if saga_id not in saga_store:
            print(f"Saga {saga_id} not found")
            return
            
        saga = saga_store[saga_id]
        saga.add_step("ORDER_COMPLETED", "SUCCESS", data)
        saga.update_status(SagaStatus.COMPLETED)
        
        print(f"Saga {saga_id} completed successfully")
    
    def handle_order_cancelled(self, data):
        """Handle order cancelled event"""
        print(f"Received order cancelled event: {data}")
        saga_id = data.get("saga_id")
        
        if saga_id not in saga_store:
            print(f"Saga {saga_id} not found")
            return
            
        saga = saga_store[saga_id]
        saga.add_step("ORDER_CANCELLED", "SUCCESS", data)
        saga.update_status(SagaStatus.COMPENSATED)
        
        print(f"Saga {saga_id} compensated successfully") 