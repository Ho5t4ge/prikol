from db.db_config import Base
from sqlalchemy.types import Integer, String
from sqlalchemy import Column, and_
from db.db_config import session_maker
from typing import List


class WellInfo(Base):
    __tablename__ = 'well_info'
    __table_args__ = {"schema": "well"}
    wellid = Column(Integer, primary_key=True)
    well_name = Column(String)
    field_id = Column(Integer)
    enterp_id = Column(Integer)
    base_name = Column(String)


def get_well_info_by_well_name(well_names, field_ids, session=session_maker()) -> List[WellInfo]:
    return session.query(WellInfo).where(
        and_(WellInfo.well_name.in_(well_names), WellInfo.field_id.in_(field_ids))).all()
