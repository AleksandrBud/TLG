from sqlalchemy.sql import exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, BIGINT, DateTime
from sqlalchemy.orm import sessionmaker


class TlgChanel(declarative_base()):
    # Класс канала
    __tablename__ = 'tlg'
    chat = Column(BIGINT, primary_key=True)
    type = Column(String(20))
    last_date = Column(DateTime)
    name = Column(String(50))

    def __init__(self, i_chat, i_type, i_last_date, i_name):
        self.chat = i_chat
        self.type = i_type
        self.last_date = i_last_date
        self.name = i_name


def unique_record(chat: str, session: sessionmaker) -> bool:
    # Проверка на уникальность чата
    result = session.query(exists().where(TlgChanel.chat == chat)).scalar()
    return result


def get_last_date(chat: str, session: sessionmaker) -> DateTime:
    # Последнее просмотренное сообщение
    result = session.query(TlgChanel).filter(TlgChanel.chat == chat)
    return result[0].last_date


def update_last_date(chat: str, new_last_date: DateTime, session: sessionmaker):
    try:
        result = session.query(TlgChanel).filter(TlgChanel.chat == chat). \
            update({TlgChanel.last_date: new_last_date}, synchronize_session=False)
        session.commit()
    except Exception as e:
        session.rollback()
        print('dbError:', chat, new_last_date, e)


def get_type(chat: str, session: sessionmaker) -> String:
    # Получаем тип чата
    result = session.query(TlgChanel).filter(TlgChanel.chat == chat)
    return result[0].type


def new_record(element: dict, session: sessionmaker) -> int:
    # Добавление чата
    try:
        record = TlgChanel(element['chat'], element['type'], element['last_date'], element['name'])
        session.add(record)
        session.commit()
        return 0
    except Exception as e:
        session.rollback()
        print('dbError:', element['chat'], element['name'], e)
        return 1
