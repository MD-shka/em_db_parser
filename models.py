# type: ignore
from datetime import datetime
from sqlalchemy import Column, String, Integer, Date, DateTime
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class SpimexTradingResult(Base):
    __tablename__ = "spimex_trading_result"

    id = Column(Integer, primary_key=True)
    exchange_product_id = Column(String, nullable=False)
    exchange_product_name = Column(String, nullable=False)
    oil_id = Column(String, nullable=False)
    delivery_basis_id = Column(String, nullable=False)
    delivery_basis_name = Column(String, nullable=False)
    delivery_type_id = Column(String, nullable=False)
    volume = Column(Integer, nullable=False)
    total = Column(Integer, nullable=False)
    count = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    created_on = Column(DateTime, default=datetime.now)
    updated_on = Column(DateTime, default=datetime.now, onupdate=datetime.now)
