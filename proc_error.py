from sqlalchemy.sql import exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import and_
from sqlalchemy import Column, String, BIGINT, Integer
from sqlalchemy.orm import sessionmaker


class TlgError(declarative_base()):
    # Класс для ошибок
    __tablename__ = 'tlg_error'
    chat = Column(BIGINT, primary_key=True)
    message = Column(BIGINT, primary_key=True)
    status = Column(Integer)
    desc = Column(String(500))

    def __init__(self, chat, message, status, desc):
        self.chat = chat
        self.message = message
        self.status = status
        self.desc = desc


def unique_record_err(chat: int, mess: int, session: sessionmaker):
    # Проверка на уникальность ошибки
    result = session.query(exists().where(and_(TlgError.chat == chat, TlgError.message == mess))).scalar()
    return result


def new_record_err(element: dict, session: sessionmaker) -> int:
    try:
        if unique_record_err(element['chat'], element['message'], session):
            record = TlgError(element['chat'], element['message'], element['status'], element['desc'])
            session.add(record)
            session.commit()
            return 0
    except Exception as e:
        print('Errors new record add: ', element['chat'], element['message'], element['status'], element['desc'])
        print(e)
        session.rollback()
        return 1


def delete_record_err(chat: int, mess: int, session: sessionmaker):
    try:
        session.query(TlgError).filter(and_(TlgError.chat == chat, TlgError.message == mess)).delete()
        session.commit()
    except Exception as e:
        print('Delet record error: ', chat, mess)
        session.rollback()
        print('dbError:', chat, e)


def get_status_mess(chat: int, mess_id: int, session: sessionmaker) -> str:
    # Получаем статус сообщения
    result = session.query(TlgError).filter(and_(TlgError.chat == chat, TlgError.message == mess_id))
    ret = 'OK'
    try:
        if result[0].status > 1:
            ret = 'ERROR'
    except Exception as e:
        print(e)
        ret = 'OK'
    return ret


def get_chat_errors(chat: int, session: sessionmaker) -> list:
    # Получаем статус сообщения
    result = session.query(TlgError).filter(TlgError.chat == chat)
    list_err_mes = [row.message for row in result]
    return list_err_mes


def process_exeption(i_id, i_msg, i_status, i_desc, i_session):
    # Обработка исключений
    ex_r = {'chat': i_id,
            'message': i_msg,
            'status': i_status,
            'desc': i_desc}
    new_record_err(ex_r, i_session)
