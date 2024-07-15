from typing import List

from db.db_config import Base
from sqlalchemy.types import Integer, Float
from sqlalchemy import Column
from db.db_config import session_maker


class GeologicalCharacteristics(Base):
    __tablename__ = 'geologcal_characteristics'
    __table_args__ = {"schema": "well"}
    id = Column(Integer, primary_key=True)
    field_id = Column(Integer)
    stratum_id = Column(Integer)
    density_oil_stratum = Column(Float)


def get_oil_density_by_field_id_and_stratum_id(field_id, stratum_id, session=session_maker()) -> List[GeologicalCharacteristics]:
    return session.query(GeologicalCharacteristics).where(
        GeologicalCharacteristics.field_id.__eq__(field_id) & GeologicalCharacteristics.stratum_id.__eq__(
            stratum_id)).all()
