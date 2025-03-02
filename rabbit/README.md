# RabbitMQ Microservices Example

A demonstration of microservices architecture using RabbitMQ for communication between services. This project showcases different RabbitMQ message exchange patterns in a e-commerce style application with user, product, and order services.

## Service Ports

- User Service: 8000
- Product Service: 8001
- Order Service: 8002
- RabbitMQ: 5672

## Project Structure

```
├── docker-compose.yml    # Docker configuration for PostgreSQL and RabbitMQ
├── user/                 # User microservice
├── product/              # Product microservice
└── order/                # Order microservice
```

Each microservice follows a similar structure:
- `main.py` - FastAPI application entry point
- `routes.py` - API endpoints
- `models.py` - Database models
- `schemas.py` - Pydantic schemas for request/response validation
- `crud.py` - Database operations
- `database.py` - Database connection setup
- `rabbitmq.py` - RabbitMQ messaging implementation
- `mqlistener.py` - RabbitMQ message consumers

## RabbitMQ Messaging Patterns

This project implements four different RabbitMQ messaging patterns:

### 1. Default Exchange (Direct Queue)
- **Implementation**: `publish_default()` and `consume_default()`
- **Use Case**: Simple point-to-point messaging
- **Example Queues**:
  - `order.create` - Create a new order
  - `order.update` - Update order status
  - `payment.process` - Process payment

### 2. Fanout Exchange
- **Implementation**: `publish_fanout()` and `consume_fanout()`
- **Use Case**: Broadcast messages to multiple consumers
- **Example Exchanges**:
  - `EX_ORDER` - Order events
  - `EX_PRODUCT` - Product events
  - `EX_USER` - User events

### 3. Topic Exchange
- **Implementation**: `publish_topic()` and `consume_topic()`
- **Use Case**: Route messages based on patterns
- **Example Routing Keys**:
  - `order.create` - Create order events
  - `user.check` - User verification events
  - `update.*` - Any update events

### 4. RPC (Remote Procedure Call)
- **Implementation**: `publish_rpc()` and `consume_rpc()`
- **Use Case**: Request-response pattern
- **Example Queues**:
  - `order_create` - Create order procedure
  - `product_stock_update` - Update product stock procedure
  - `order_status_update` - Update order status procedure
  - `user_check` - Check user balance procedure

## Order Flow (Current Implementation - RPC)

1. User creates an order via Order Service API
2. Order Service creates a new order record with status "PROCESSING"
3. Order Service sends RPC request to Product Service to check stock
4. Product Service checks if stock is available
5. If stock is available, Product Service sends RPC request to User Service to check balance
6. User Service checks if user has sufficient balance
7. If balance is sufficient, User Service deducts the amount and returns success
8. Product Service updates stock and sends status update to Order Service
9. Order Service updates the order status to "SUCCESS" or "FAILED"

## Installation and Setup

1. Start the infrastructure:
```
docker-compose up -d
```

2. Start each microservice (in separate terminals):
```
cd user
python main.py

cd product
python main.py

cd order
python main.py
```

## Dependencies

Each service requires:
- FastAPI
- SQLAlchemy
- Pydantic
- pika (RabbitMQ client)
- PostgreSQL database
