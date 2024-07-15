from db.db_config import Base
from sqlalchemy.types import Integer, Date, Float, Boolean
from sqlalchemy import Column, ForeignKey, func
from sqlalchemy.orm import relationship
from db.db_config import session_maker
from typing import List
from utils.config import search_date


class TechnologyWell(Base):
    __tablename__ = 'technology_well'
    __table_args__ = {"schema": "well"}
    wellid = Column(Integer)
    field_id = Column(Integer)
    stratum_id = Column(Integer)
    pump_id = Column(Integer, ForeignKey('wellref.pump.id'))
    id = Column(Integer, primary_key=True)
    pump = relationship('Pump', backref='technology_wells')
    date_tech = Column(Date)
    rate_liquid_m3 = Column(Float)
    water_cut_m3 = Column(Float)
    gas_factor = Column(Float)
    flag_stratum = Column(Boolean)


def get_technology_well_by_wells_ids(wellids, session=session_maker()) -> List[TechnologyWell]:
    return session.query(TechnologyWell).where(
        TechnologyWell.wellid.in_(wellids) & (TechnologyWell.date_tech.__eq__(search_date)) & (
            TechnologyWell.flag_stratum.__eq__(False))
    ).all()


def get_technology_well_by_parts_by_well_ids(wellids, session=session_maker()) -> List[TechnologyWell]:
    return session.query(TechnologyWell).where(
        TechnologyWell.wellid.__eq__(wellids) & (TechnologyWell.date_tech.__eq__(search_date)) & (
            TechnologyWell.flag_stratum.__eq__(True))
    ).all()
