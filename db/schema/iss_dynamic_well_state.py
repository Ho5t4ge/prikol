from typing import List

from db.db_config import Base
from sqlalchemy.types import Integer, Date, String
from sqlalchemy import Column
from db.db_config import session_maker


class IssDynamicWellState(Base):
    __tablename__ = 'iss_dynamic_well_state'
    __table_args__ = {"schema": "wi_web"}
    wellid = Column(Integer, primary_key=True)
    date_begin_time = Column(Date)
    date_end_time = Column(Date)
    w_state = Column(String)


def get_well_states_by_wells_ids(well_ids,search_date, session=session_maker()) -> List[IssDynamicWellState]:
    return session.query(IssDynamicWellState).where(
        IssDynamicWellState.wellid.in_(well_ids) &
        (IssDynamicWellState.date_begin_time <= search_date) &
        (IssDynamicWellState.date_end_time >= search_date)
    ).all()
