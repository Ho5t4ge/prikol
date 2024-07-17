from typing import List, Type
from db.abstract_db import AbstractDB
from sqlalchemy.types import Integer, Float
from sqlalchemy import Column
from db.schema.base import base


class GeologicalCharacteristics(base):
    __tablename__ = 'geologcal_characteristics'
    __table_args__ = {"schema": "well"}
    id = Column(Integer, primary_key=True)
    field_id = Column(Integer)
    stratum_id = Column(Integer)
    density_oil_stratum = Column(Float)


class GeologicalCharacteristicsSchema:
    def __init__(self, db: AbstractDB):
        self.db = db

    def get_oil_density_by_field_id_and_stratum_id(self, field_id, stratum_id, session=None) -> List[Type[GeologicalCharacteristics]]:
        if session is None:
            session = self.db.get_session()
        return session.query(GeologicalCharacteristics).where(
            GeologicalCharacteristics.field_id.__eq__(field_id) & GeologicalCharacteristics.stratum_id.__eq__(
                stratum_id)).all()
