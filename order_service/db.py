from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine("postgresql://postgres:password@postgres:5432/orders")
SessionLocal = sessionmaker(bind=engine)