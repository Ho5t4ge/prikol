from db.db_config import Base
from sqlalchemy.types import Integer, String
from sqlalchemy import Column
from db.db_config import session_maker
from typing import List


class IssDobZakDobData(Base):
    __tablename__ = 'iss_dobzak_dob_data'
    __table_args__ = {"schema": "wi_web"}
    wellid = Column(Integer, primary_key=True)


def get_dob_data_by_well_id(well_ids, session=session_maker()) -> List[IssDobZakDobData]:
    return session.query(IssDobZakDobData).where(
        IssDobZakDobData.wellid.in_(well_ids)).all()
