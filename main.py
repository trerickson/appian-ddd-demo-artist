import os
import requests
from typing import List, Optional
from datetime import date
from fastapi import FastAPI, HTTPException, Depends, Body
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session

# --- 1. INFRASTRUCTURE LAYER (Database) ---
DATABASE_URL = os.getenv("DATABASE_URL")

# Railway Fix: Convert 'postgres://' to 'postgresql://' for SQLAlchemy
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Fallback for local testing if env var is missing
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./test.db" # Create local file if no DB found

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- 2. DOMAIN MODELS (The Database Tables) ---
class CustomerModel(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    email = Column(String, unique=True, index=True)
    
    # Relationship: One Customer has Many Orders
    orders = relationship("OrderModel", back_populates="customer", cascade="all, delete-orphan")

class OrderModel(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    order_date = Column(Date)
    status = Column(String, default="Draft")
    total_amount = Column(Float, default=0.0)

    # Relationships
    customer = relationship("CustomerModel", back_populates="orders")
    items = relationship("LineItemModel", back_populates="order", cascade="all, delete-orphan")

class LineItemModel(Base):
    __tablename__ = "line_items"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("orders.id"))
    product_name = Column(String)
    quantity = Column(Integer)
    price = Column(Float)

    # Relationship
    order = relationship("OrderModel", back_populates="items")

# Create Tables on Startup
Base.metadata.create_all(bind=engine)

# --- 3. PYDANTIC DTOs (The Fix is Here) ---
class LineItemDTO(BaseModel):
    product_name: str
    quantity: int
    price: float
    
    class Config:
        orm_mode = True

class OrderDTO(BaseModel):
    # FIX: Added '= None' so you don't have to send 'id: null' in Postman
    id: Optional[int] = None 
    customer_id: int
    order_date: date
    status: str
    # FIX: Added '= None' so the API calculates it, not the user
    total_amount: Optional[float] = None 
    items: List[LineItemDTO] = []

    class Config:
        orm_mode = True

class CustomerDTO(BaseModel):
    # FIX: Added '= None'
    id: Optional[int] = None
    name: str
    email: str

    class Config:
        orm_mode = True

class OrderUpdateDTO(BaseModel):
    status: str

# --- 4. API CONFIGURATION ---
app = FastAPI()

# Appian Webhook Config
APPIAN_WEBHOOK_URL = "https://<YOUR_APPIAN_SITE>/suite/webapi/refresh-order"
APPIAN_API_KEY = "<YOUR_APPIAN_API_KEY>"

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- 5. HELPER FUNCTION ---
def trigger_appian_webhook(order_id):
    try:
        # In production, use a background task. For demo, a short timeout is fine.
        requests.post(
            APPIAN_WEBHOOK_URL, 
            json={"id": order_id},
            headers={"Appian-API-Key": APPIAN_API_KEY},
            timeout=2 
        )
        print(f"Webhook sent for Order {order_id}")
    except Exception as e:
        print(f"Webhook failed: {e}")

# --- 6. API ENDPOINTS ---

# --- CUSTOMERS ---
@app.post("/customers/", response_model=CustomerDTO)
def create_customer(customer: CustomerDTO, db: Session = Depends(get_db)):
    db_cust = CustomerModel(name=customer.name, email=customer.email)
    db.add(db_cust)
    db.commit()
    db.refresh(db_cust)
    return db_cust

@app.get("/customers/", response_model=List[CustomerDTO])
def get_customers(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return db.query(CustomerModel).offset(skip).limit(limit).all()

# --- ORDERS (The Aggregate Root) ---

@app.post("/orders/", response_model=OrderDTO)
def create_order(order: OrderDTO, db: Session = Depends(get_db)):
    # 1. Create the Order
    db_order = OrderModel(
        customer_id=order.customer_id,
        order_date=order.order_date,
        status=order.status,
        total_amount=0
    )
    db.add(db_order)
    db.commit() 
    db.refresh(db_order)

    # 2. Add Line Items (Transactional Consistency)
    total = 0.0
    for item in order.items:
        db_item = LineItemModel(
            order_id=db_order.id,
            product_name=item.product_name,
            quantity=item.quantity,
            price=item.price
        )
        total += (item.quantity * item.price)
        db.add(db_item)
    
    # 3. Update Total on Order
    db_order.total_amount = total
    db.commit()
    db.refresh(db_order)
    
    # 4. Fire Webhook (Tell Appian we have a new order)
    trigger_appian_webhook(db_order.id)
    
    return db_order

@app.get("/orders/", response_model=List[OrderDTO])
def get_orders(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    # Batching for Appian
    return db.query(OrderModel).offset(skip).limit(limit).all()

@app.get("/orders/{order_id}", response_model=OrderDTO)
def get_order(order_id: int, db: Session = Depends(get_db)):
    return db.query(OrderModel).filter(OrderModel.id == order_id).first()

@app.patch("/orders/{order_id}")
def update_order_status(order_id: int, update: OrderUpdateDTO, db: Session = Depends(get_db)):
    """
    The 'Rogue Update' Endpoint.
    """
    db_order = db.query(OrderModel).filter(OrderModel.id == order_id).first()
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Update status
    db_order.status = update.status
    db.commit()
    
    # Trigger Webhook
    trigger_appian_webhook(order_id)
    
    return {"message": "Status updated", "id": order_id, "new_status": update.status}
