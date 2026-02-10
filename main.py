import os
import requests
from typing import List, Optional
from datetime import date
from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session

# --- 1. INFRASTRUCTURE LAYER (Database) ---
DATABASE_URL = os.getenv("DATABASE_URL")

# Handle standard Postgres URL format for SQLAlchemy
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- 2. DOMAIN MODELS (The Tables) ---
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

# --- 3. DTOs (Data Transfer Objects for API) ---
class LineItemDTO(BaseModel):
    product_name: str
    quantity: int
    price: float

class OrderDTO(BaseModel):
    customer_id: int
    order_date: date
    status: str
    items: List[LineItemDTO] = [] # Nested Writes supported

class CustomerDTO(BaseModel):
    name: str
    email: str

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
        # Note: In production, use a background task (e.g., Celery) for this
        requests.post(
            APPIAN_WEBHOOK_URL, 
            json={"id": order_id},
            headers={"Appian-API-Key": APPIAN_API_KEY},
            timeout=5
        )
        print(f"Webhook sent for Order {order_id}")
    except Exception as e:
        print(f"Webhook failed: {e}")

# --- 6. API ENDPOINTS ---

# --- CUSTOMERS ---
@app.post("/customers/")
def create_customer(customer: CustomerDTO, db: Session = Depends(get_db)):
    db_cust = CustomerModel(name=customer.name, email=customer.email)
    db.add(db_cust)
    db.commit()
    db.refresh(db_cust)
    return db_cust

@app.get("/customers/")
def get_customers(
    skip
