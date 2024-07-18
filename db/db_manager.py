#  Copyright (c) 2010-2024. LUKOIL-Engineering Limited KogalymNIPINeft Branch Office in Tyumen
#  Данным программным кодом владеет Филиал ООО "ЛУКОЙЛ-Инжиниринг" "КогалымНИПИнефть" в г.Тюмени

from db.abstract_db import AbstractDB
from db.schema.technology_well import TechnologyWellSchema
from db.schema.well_info import WellInfoSchema
from db.schema.iss_dynamic_well_state import IssDynamicWellStateSchema
from db.schema.iss_dobzak_zak_tm import IssDobZakZakTmSchema
from db.schema.geological_characteristics import GeologicalCharacteristicsSchema
from db.schema.base import base
from utils.config import Config
import cx_Oracle
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session


class DB(AbstractDB):
    technology_well_schema: TechnologyWellSchema
    well_info_schema: WellInfoSchema
    iss_dynamic_well_state_schema: IssDynamicWellStateSchema
    iss_dob_zak_zak_tm_schema: IssDobZakZakTmSchema
    geological_characteristics: GeologicalCharacteristicsSchema

    def __init__(self, config: Config):
        if config.use_external_oracle:
            cx_Oracle.init_oracle_client(config.external_oracle_path)
        dsn = cx_Oracle.makedsn(config.db_host, config.db_port, service_name=config.db_name)
        connection_string = f'oracle+cx_oracle://{config.db_user}:{config.db_pass}@{dsn}'
        self.engine = create_engine(connection_string, max_identifier_length=128, pool_pre_ping=True, pool_size=20,
                                    max_overflow=0)
        self.session_maker = sessionmaker(autocommit=False, autoflush=False, bind=self.engine, expire_on_commit=False)
        self.prepare()

    def get_session(self) -> Session:
        return self.session_maker()

    def prepare(self):
        self.technology_well_schema = TechnologyWellSchema(self)
        self.well_info_schema = WellInfoSchema(self)
        self.iss_dynamic_well_state_schema = IssDynamicWellStateSchema(self)
        self.iss_dob_zak_zak_tm_schema = IssDobZakZakTmSchema(self)
        self.geological_characteristics = GeologicalCharacteristicsSchema(self)
        base.metadata.create_all(self.engine)
