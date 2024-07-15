from db.db_config import Base
from sqlalchemy.types import Integer, String
from sqlalchemy import Column


class Pump(Base):
    __tablename__ = 'pump'
    __table_args__ = {"schema": "wellref"}
    name = Column(String)
    id = Column(Integer, primary_key=True)
