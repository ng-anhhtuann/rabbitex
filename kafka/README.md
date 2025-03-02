# Microservices with Kafka and Saga Orchestration

## Services
- user : 8000
- product : 8001
- order : 8002
- orchestrator : 8003

## Architecture

This project demonstrates a microservices architecture using Kafka for message-based communication and the Saga Orchestration pattern for distributed transactions.

### Saga Orchestration Pattern

The Saga Orchestration pattern is implemented to ensure transaction consistency across microservices. The orchestrator service coordinates the entire transaction flow and handles compensating transactions in case of failures.

## Transaction Flow

1. Client sends order request to the Orchestrator
2. Orchestrator creates a saga and sends a create order command to Order service
3. Order service creates an order and sends an order created event
4. Orchestrator sends a reserve product command to Product service
5. Product service reserves the product and sends a product reserved event
6. Orchestrator sends a process payment command to User service
7. User service processes the payment and sends a payment processed event
8. Orchestrator sends a complete order command to Order service
9. Order service completes the order and sends an order completed event

## Compensation Flow (Rollback)

If any step fails, the orchestrator initiates compensating transactions:

1. If product reservation fails, cancel the order
2. If payment fails, release the product and cancel the order

## Running the Application

```bash
docker-compose up -d
```

## API Endpoints

### Orchestrator Service (8003)
- POST /orders - Create a new order and start the saga
- GET /sagas/{saga_id} - Get the current state of a saga
- GET /sagas - Get all sagas

### Order Service (8002)
- GET /orders - Get all orders
- GET /orders/{order_id} - Get an order by ID
- POST /orders - Create a new order
- DELETE /orders/{order_id} - Delete an order

### Product Service (8001)
- GET /products - Get all products
- GET /products/{product_id} - Get a product by ID
- POST /products - Create a new product
- PUT /products/{product_id} - Update a product
- DELETE /products/{product_id} - Delete a product

### User Service (8000)
- GET /users - Get all users
- GET /users/{user_id} - Get a user by ID
- POST /users - Create a new user
- PUT /users/{user_id} - Update a user
- DELETE /users/{user_id} - Delete a user
