from sqlalchemy import Column, Integer, String
from database import Base

class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, index=True, nullable=False)
    quantity = Column(Integer, index=True, nullable=False)
    owner_id = Column(Integer, index=True,nullable=False)
    status = Column(String, index=True, nullable=False) # PROCESSING or FAILED or SUCCESS
