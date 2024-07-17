from db.schema.base import base
from sqlalchemy.types import Integer, Date, Float
from sqlalchemy import Column
from typing import List, Type
from db.abstract_db import AbstractDB


class IssDobZakZakTm(base):
    __tablename__ = 'iss_dobzak_zak_tm'
    __table_args__ = {"schema": "wi_web"}
    wellid = Column(Integer, primary_key=True)
    date_prod = Column(Date)
    qz_m3 = Column(Float)


class IssDobZakZakTmSchema:
    def __init__(self, db: AbstractDB):
        self.db = db

    def get_iss_dob_zak_zak_tm(self, well_ids, search_date, session=None) -> List[Type[IssDobZakZakTm]]:
        if session is None:
            session = self.db.get_session()
        return session.query(IssDobZakZakTm).where(
            IssDobZakZakTm.wellid.in_(well_ids) & (IssDobZakZakTm.date_prod.__eq__(search_date))).all()
