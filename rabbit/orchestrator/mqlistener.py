from rabbitmq import consume_saga_orchestrator
import models, database
import crud
import threading

def start_listener():
    print("Starting saga orchestrator listener...")
    
    def event_handler(saga_id, data):
        """Handle an event by delegating to the appropriate handler"""
        db = database.SessionLocal()
        
        try:
            service = data.get("service")
            action = data.get("action")
            result = data.get("result")
            
            if not all([service, action, result]):
                print(f"Invalid event data: {data}")
                return
            
            pattern = f"{service}.{action}.{result}"
            
            if pattern in crud.event_handlers:
                crud.event_handlers[pattern](db, saga_id, data)
            else:
                print(f"No handler found for pattern: {pattern}")
        
        finally:
            db.close()
    
    # Start consuming saga events
    consume_saga_orchestrator({"*": event_handler})

def start_listener_thread():
    """Start the listener in a separate thread"""
    thread = threading.Thread(target=start_listener, daemon=True)
    thread.start()
    return thread 