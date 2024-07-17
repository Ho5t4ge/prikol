from abc import ABC, abstractmethod
from sqlalchemy.orm import Session, declarative_base
from exceptions.session_exception import SessionException


class AbstractDB(ABC):
    @abstractmethod
    def get_session(self) -> Session:
        raise SessionException()
