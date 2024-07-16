from db.db_config import Base
from sqlalchemy.types import Integer, Date, Float
from sqlalchemy import Column
from db.db_config import session_maker
from typing import List
from utils.config import search_date


class IssDobZakZakTm(Base):
    __tablename__ = 'iss_dobzak_zak_tm'
    __table_args__ = {"schema": "wi_web"}
    wellid = Column(Integer, primary_key=True)
    date_prod = Column(Date)
    qz_m3 = Column(Float)


def get_iss_dob_zak_zak_tm(well_ids, session=session_maker()) -> List[IssDobZakZakTm]:
    return session.query(IssDobZakZakTm).where(
        IssDobZakZakTm.wellid.in_(well_ids) & (IssDobZakZakTm.date_prod.__eq__(search_date))).all()
