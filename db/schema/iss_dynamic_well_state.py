from typing import List, Type
from db.abstract_db import AbstractDB
from db.schema.base import base
from sqlalchemy.types import Integer, Date, String
from sqlalchemy import Column


class IssDynamicWellState(base):
    __tablename__ = 'iss_dynamic_well_state'
    __table_args__ = {"schema": "wi_web"}
    wellid = Column(Integer, primary_key=True)
    date_begin_time = Column(Date)
    date_end_time = Column(Date)
    w_state = Column(String)


class IssDynamicWellStateSchema:
    def __init__(self, db: AbstractDB):
        self.db = db

    def get_well_states_by_wells_ids(self, well_ids, search_date, session=None) -> List[Type[IssDynamicWellState]]:
        if session is None:
            session = self.db.get_session()
        return session.query(IssDynamicWellState).where(
            IssDynamicWellState.wellid.in_(well_ids) &
            (IssDynamicWellState.date_begin_time <= search_date) &
            (IssDynamicWellState.date_end_time >= search_date)
        ).all()
