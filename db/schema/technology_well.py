from db.schema.base import base
from sqlalchemy.types import Integer, Date, Float, Boolean
from sqlalchemy import Column
from typing import List, Type
from db.abstract_db import AbstractDB


class TechnologyWell(base):
    __tablename__ = 'technology_well'
    __table_args__ = {"schema": "well"}
    wellid = Column(Integer)
    field_id = Column(Integer)
    stratum_id = Column(Integer)
    id = Column(Integer, primary_key=True)
    date_tech = Column(Date)
    rate_liquid_m3 = Column(Float)
    water_cut_m3 = Column(Float)
    gas_factor = Column(Float)
    flag_stratum = Column(Boolean)


class TechnologyWellSchema:
    def __init__(self, db: AbstractDB):
        self.db = db

    def get_technology_well_by_wells_ids(self, wellids, search_date, session=None) -> List[Type[TechnologyWell]]:
        if session is None:
            session = self.db.get_session()
        return session.query(TechnologyWell).where(
            TechnologyWell.wellid.in_(wellids) & (TechnologyWell.date_tech.__eq__(search_date)) & (
                TechnologyWell.flag_stratum.__eq__(False))
        ).all()

    def get_technology_well_by_parts_by_wells_ids(self, wellids, search_date, session=None) -> List[Type[TechnologyWell]]:
        if session is None:
            session = self.db.get_session()
        return session.query(TechnologyWell).where(
            TechnologyWell.wellid.__eq__(wellids) & (TechnologyWell.date_tech.__eq__(search_date)) & (
                TechnologyWell.flag_stratum.__eq__(True))
        ).all()
