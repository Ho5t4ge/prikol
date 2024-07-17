from db.schema.base import base
from sqlalchemy.types import Integer, String
from sqlalchemy import Column, and_
from typing import List, Type
from db.abstract_db import AbstractDB


class WellInfo(base):
    __tablename__ = 'well_info'
    __table_args__ = {"schema": "well"}
    wellid = Column(Integer, primary_key=True)
    well_name = Column(String)
    field_id = Column(Integer)
    enterp_id = Column(Integer)
    base_name = Column(String)


class WellInfoSchema:
    def __init__(self, db: AbstractDB):
        self.db = db

    def get_well_info_by_well_name(self, well_names, field_ids,session=None) -> List[Type[WellInfo]]:
        if session is None:
            session = self.db.get_session()
        return session.query(WellInfo).where(
            and_(WellInfo.well_name.in_(well_names), WellInfo.field_id.in_(field_ids))).all()
