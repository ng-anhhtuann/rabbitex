# Microservices with RabbitMQ and Saga Orchestration

## Services
- User Service: 8000
- Product Service: 8001
- Order Service: 8002
- Orchestrator Service: 8003

## Architecture

This project demonstrates a microservices architecture using RabbitMQ for message-based communication and the Saga Orchestration pattern for distributed transactions. The orchestrator service coordinates the entire transaction flow and handles compensating transactions in case of failures.

### Saga Orchestration Pattern

The Saga Orchestration pattern is implemented using RabbitMQ Topic Exchange to ensure transaction consistency across microservices. The orchestrator service coordinates the entire transaction flow and handles compensating transactions in case of failures.

## Transaction Flow

1. Client sends order request to the Orchestrator
2. Orchestrator creates a saga and initiates the process:
   - Creates saga record with status "STARTED"
   - Sends check_stock command to Product service

3. Product Service Flow:
   - Receives check_stock command
   - Verifies product availability
   - Sends success/failure event back to orchestrator

4. User Service Flow (if product available):
   - Receives check_balance command
   - Verifies user has sufficient balance
   - If successful, receives update_balance command
   - Processes payment and sends result event

5. Product Service Update (if payment successful):
   - Receives update_stock command
   - Updates product quantity
   - Sends success/failure event

6. Order Service Completion:
   - Receives update_status command
   - Updates order status to SUCCESS/FAILED
   - Sends completion event

## Compensation Flow (Rollback)

If any step fails, the orchestrator initiates compensating transactions:

1. If product check fails:
   - Update order status to FAILED

2. If payment fails:
   - Update order status to FAILED

3. If stock update fails:
   - Refund payment to user
   - Update order status to FAILED

## Running the Application

```bash
# Start RabbitMQ and PostgreSQL
docker-compose up -d

# Start services (in separate terminals)
cd user
python main.py

cd product
python main.py

cd order
python main.py

cd orchestrator
python main.py
```

## API Endpoints

### Orchestrator Service (8003)
- POST /orders - Create a new order and start the saga
  ```json
  {
    "product_id": 1,
    "owner_id": 1,
    "quantity": 2
  }
  ```
- GET /sagas/{saga_id} - Get saga status
- GET /sagas/order/{order_id} - Get saga by order ID
- GET /sagas - List all sagas

### Order Service (8002)
- GET /orders - Get all orders
- GET /orders/{order_id} - Get order by ID
- POST /orders - Create order
- DELETE /orders/{order_id} - Delete order

### Product Service (8001)
- GET /products - Get all products
- GET /products/{product_id} - Get product by ID
- POST /products - Create product
- PUT /products/{product_id} - Update product
- DELETE /products/{product_id} - Delete product

### User Service (8000)
- GET /users - Get all users
- GET /users/{user_id} - Get user by ID
- POST /users - Create user
- PUT /users/{user_id} - Update user
- DELETE /users/{user_id} - Delete user

## Message Flow

### Topic Exchange Patterns
- Commands: `saga.{sagaId}.command.{service}.{action}`
- Events: `saga.{sagaId}.event.{service}.{action}.{result}`

### Example Message Flow
1. Command: `saga.123.command.product.check_stock`
2. Event: `saga.123.event.product.check_stock.success`
3. Command: `saga.123.command.user.check_balance`
4. Event: `saga.123.event.user.check_balance.success`
5. Command: `saga.123.command.user.update_balance`
6. Event: `saga.123.event.user.update_balance.success`
7. Command: `saga.123.command.product.update_stock`
8. Event: `saga.123.event.product.update_stock.success`
9. Command: `saga.123.command.order.update_status`
10. Event: `saga.123.event.order.update_status.success`

## Dependencies

- FastAPI
- SQLAlchemy
- Pydantic
- RabbitMQ (pika)
- PostgreSQL
- Python-dotenv

## Project Structure
