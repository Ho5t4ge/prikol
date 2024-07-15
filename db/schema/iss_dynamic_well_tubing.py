from db.db_config import Base
from sqlalchemy.types import Integer, String, Float
from sqlalchemy import Column


class IssDynamicWellTubing(Base):
    __tablename__ = 'iis_dynamic_well_tubing'
    __table_args__ = {"schema": "wi_web"}
    length_stage_tubing = Column(Float)
    wellid = Column(Integer, primary_key=True)
