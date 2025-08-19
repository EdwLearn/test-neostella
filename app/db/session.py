import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

DATABASE_URl =  os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://app:app@postgres:5432/app"
)

engine = create_engine(DATABASE_URl, pool_pre_ping=True)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

class Base(DeclarativeBase):
    pass