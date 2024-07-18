#  Copyright (c) 2010-2024. LUKOIL-Engineering Limited KogalymNIPINeft Branch Office in Tyumen
#  Данным программным кодом владеет Филиал ООО "ЛУКОЙЛ-Инжиниринг" "КогалымНИПИнефть" в г.Тюмени

from abc import ABC, abstractmethod
from sqlalchemy.orm import Session, declarative_base
from exceptions.session_exception import SessionException


class AbstractDB(ABC):
    @abstractmethod
    def get_session(self) -> Session:
        raise SessionException()
