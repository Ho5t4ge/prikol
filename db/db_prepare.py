from db.schema import *
from db.db_config import Base, engine


def prepare_db():
    Base.metadata.create_all(engine)
