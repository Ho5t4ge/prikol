from db.db_config import Base
from sqlalchemy.types import Integer, String
from sqlalchemy import Column
from db.db_config import session_maker
from typing import List


class WellInfo(Base):
    __tablename__ = 'well_info'
    schema = 'well'
    wellid = Column(Integer, primary_key=True)
    well_name = Column(String)


def get_well_info_by_well_name(well_names, session=session_maker()) -> List[WellInfo]:
    return session.query(WellInfo).where(WellInfo.well_name.in_(well_names)).all()
