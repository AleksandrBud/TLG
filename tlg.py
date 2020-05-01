import configparser
import socks
# import time
# import json
# import telethon
import chardet
import con_db
import asyncio
from telethon.sync import TelegramClient
from sqlalchemy import create_engine, and_
from sqlalchemy import Column, String, DateTime, BIGINT, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists, select


# from telethon import connection
# from detetime import date, datetime
# from telethon.tl.functions.channels import GetParticipantsRequest
# from telethon.tl.types import ChannelParticipantsSearch
# from telethon.tl.functions.messages import GerHistoryRequest


class TlgChanel(declarative_base()):
    # Класс канала
    __tablename__ = 'tlg'
    chat = Column(BIGINT, primary_key=True)
    type = Column(String(20))
    last_date = Column(DateTime)
    name = Column(String(50))

    def __init__(self, chat, type, last_date, name):
        self.chat = chat
        self.type = type
        self.last_date = last_date
        self.name = name


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


def unique_record_err(chat: int, mess: int):
    # Проверка на уникальность ошибки
    result = session.query(exists().where(and_(TlgError.chat == chat, TlgError.message == mess))).scalar()
    return result


def new_record_err(element: object) -> object:
    try:
        if unique_record_err(element['chat'], element['message']):
            record = TlgError(element['chat'], element['message'], element['status'], element['desc'])
            session.add(record)
            session.commit()
    except Exception as e:
        print('Errors new record add: ', element['chat'], element['message'], element['status'], element['desc'])
        session.rollback()


def delete_record_err(chat: int, mess: int):
    try:
        session.query(TlgError).filter(and_(TlgError.chat == chat, TlgError.message == mess)).delete()
        session.commit()
    except Exception as e:
        print('Delet record error: ', chat, mess)
        session.rollback()
        print('dbError:', chat, e)


def unique_record(chat: str):
    # Проверка на уникальность чата
    result = session.query(exists().where(TlgChanel.chat == chat)).scalar()
    return result


def get_last_date(chat: str) -> DateTime:
    # Последнее просмотренное сообщение
    result = session.query(TlgChanel).filter(TlgChanel.chat == chat)
    return result[0].last_date


def update_last_date(chat: str, new_last_date: DateTime):
    try:
        result = session.query(TlgChanel).filter(TlgChanel.chat == chat). \
            update({TlgChanel.last_date: new_last_date}, synchronize_session=False)
        session.commit()
    except Exception as e:
        session.rollback()
        print('dbError:', chat, new_last_date, e)


def get_status_mess(chat: int, mess_id: int) -> str:
    # Получаем статус сообщения
    # result = session.query(and_(TlgError.chat == chat, TlgError.mess == mess_id))
    result = session.query(TlgError).filter(and_(TlgError.chat == chat, TlgError.message == mess_id))
    ret = 'OK'
    try:
        if result[0].status > 1:
            ret = 'ERROR'
    except Exception as e:
        ret = 'OK'
    return ret


def get_chat_errors(chat: int) -> list:
    # Получаем статус сообщения
    # result = session.query(and_(TlgError.chat == chat, TlgError.mess == mess_id))
    result = session.query(TlgError).filter(TlgError.chat == chat)
    # ret = 'OK'
    list_err_mes = [row.message for row in result]
    # try:
    #     if result[0].status > 1:
    #         ret = 'ERROR'
    # except Exception as e:
    #     ret = 'OK'
    return list_err_mes


def get_type(chat: str):
    # Получаем тип чата
    result = session.query(TlgChanel).filter(TlgChanel.chat == chat)
    return result[0].type


def new_record(element: object) -> object:
    # Добавление чата
    try:
        record = TlgChanel(element['chat'], element['type'], element['last_date'], element['name'])
        session.add(record)
        session.commit()
    except Exception as e:
        session.rollback()
        # record = TlgChanel(element['chat'], element['type'], element['last_date'], '')
        # session.add(record)
        # session.commit()
        print('dbError:', element['chat'], element['name'], e)


def process_exeption(id, msg, status, desc):
    # Обработка исключений
    ex_r = {'chat': id,
            'message': msg,
            'status': status,
            'desc': desc}
    new_record_err(ex_r)


def download(dialog_id, mess, task):
    try:
        task
        # delete_record_err(dialog_id, mess.id)
        print(f'Complate {mess.audio.attributes[1].file_name}')
    except Exception as e:
        print(dialog_id, mess.id, '1', e.__str__())
        process_exeption(dialog_id, mess.id, '1', e.__str__())


async def process_music(dialog: object):
    # Обработка чатов с музыкой
    msg_list = []
    lst_date = get_last_date(dialog.id)
    list_chat_errors = get_chat_errors(dialog.id)
    async for msg in client.iter_messages(dialog):
        if (msg.id in list_chat_errors
            or lst_date < msg.date.replace(tzinfo=None)) \
                and msg.audio:
            msg_list.append(msg)

    i = 0
    folder = f'/home/aleksandr/music/'
    for element in msg_list:
        # delete_record_err(dialog.id, element.id)
        # Определяет количество потоков загрузки
        if i >= 20:
            for n in range(1, i + 1):
                # download(dialog.id, globals()['task_%s' % n], globals()['mess_id%s' % i])
                try:
                    await globals()['task_%s' % n]
                    if globals()['mess_id%s' % n].id.isin(list_chat_errors):
                        delete_record_err(dialog.id, globals()['mess_id%s' % n].id)
                    fn = globals()['mess_id%s' % n].audio.attributes[1].file_name
                    print(f'Complate {fn}')
                except Exception as e:
                    print(dialog.id, globals()['mess_id%s' % n].id, '1', e.__str__())
                    process_exeption(dialog.id, globals()['mess_id%s' % n].id, '1', e.__str__())
            i = 0
        i = i + 1
        # arr_error = []
        try:
            # Создаем задачу на загрузку файлов
            globals()['task_%s' % i] = client.loop.create_task(element.download_media(folder))
            globals()['mess_id%s' % i] = element
            # task = client.loop.create_task(media.download_media(folder))
        except Exception as e:
            # Обработка исключения
            print(dialog.id, element.id, e.__str__())
            process_exeption(dialog.id, element.id, '1', e.__str__())
    else:
        if i != 0:
            for n in range(1, i + 1):
                # download(dialog.id, globals()['task_%s' % n], globals()['mess_id%s' % i])
                try:
                    await globals()['task_%s' % n]
                    if globals()['mess_id%s' % n].id.isin(list_chat_errors):
                        delete_record_err(dialog.id, globals()['mess_id%s' % n].id)
                    fn = globals()['mess_id%s' % n].audio.attributes[1].file_name
                    print(f'Complate {fn}')
                except Exception as e:
                    print(dialog.id, globals()['mess_id%s' % n].id, e.__str__())
                    process_exeption(dialog.id, globals()['mess_id%s' % n].id, '1', e.__str__())
    update_last_date(dialog.id, dialog.date)


async def main(type_content):
    dialogs = await client.get_dialogs()  # Получаем список диалогов
    for dialog in dialogs:
        # Новый чат добавляем
        print('Process chat - ' + dialog.name)
        if not unique_record(dialog.id):
            # Если диалога нет в нашей базе добавим его туда
            print('Add chat to list')
            mess_bytes = dialog.title.encode()
            code = chardet.detect(mess_bytes)
            det = chardet.universaldetector.UniversalDetector()
            det.feed(mess_bytes)
            if code['encoding'] is not None:
                # Если удалось определить кодировку сообщения, добавляем запись
                chat_info = {'chat': dialog.id,
                             'type': '0',
                             'last_date': dialog.date,
                             'name': mess_bytes.decode('UTF-8')}
                new_record(chat_info)
            else:
                # Если кодировка не определилась добавляем сообщение в ошибку
                chat_info = {'chat': dialog.id,
                             'type': '0',
                             'last_date': dialog.date,
                             'name': mess_bytes.decode('UTF-8')}
                new_record(chat_info)
                print(dialog.id, dialog.title)
        else:
            # Чат есть в списке пошли смотреть сообщения
            if get_type(dialog.id) == type_content:
                await process_music(dialog)
                # Обработка чатов с музыкой
                msg_list = []
                # lst_date = get_last_date(dialog.id)
                # list_chat_errors = get_chat_errors(dialog.id)
                # # async for msg in [ms for ms in client.iter_messages(dialog) if lst_date < ms.date.replace(tzinfo=None) \
                # #                                                                and ms.media]:
                # async for msg in client.iter_messages(dialog):
                #     if (msg.id in list_chat_errors
                #     or lst_date < msg.date.replace(tzinfo=None)) \
                #             and msg.audio:
                #     # if lst_date < msg.date.replace(tzinfo=None) and msg.audio:
                #
                #         # if msg.audio:
                #         # fn = msg.audio.attributes[1].file_name
                #         msg_list.append(msg)
                #     # stat_mess = get_status_mess(dialog.id, msg.id)
                #     # if (lst_date < msg.date.replace(tzinfo=None)) or (stat_mess == 'ERROR'):
                #     #     # Дата сообщение больше последней просмотренной
                #     #     if msg.media:
                #     #         # Сообщение содержит медиа контент, добавим в список для обработки
                #     #         msg_list.append(msg)
                # i = 0
                # folder = f'/home/aleksandr/{type_content}/'
                # for element in msg_list:
                #     # delete_record_err(dialog.id, element.id)
                #     # Определяет количество потоков загрузки
                #     if i >= 20:
                #         for n in range(1, i + 1):
                #             # download(dialog.id, globals()['task_%s' % n], globals()['mess_id%s' % i])
                #             try:
                #                 await globals()['task_%s' % n]
                #                 # delete_record_err(dialog.id, globals()['mess_id%s' % i].id)
                #                 fn = globals()['mess_id%s' % n].audio.attributes[1].file_name
                #                 print(f'Complate {fn}')
                #             except Exception as e:
                #                 print(dialog.id, globals()['mess_id%s' % n], '1', e.__str__())
                #                 process_exeption(dialog.id, globals()['mess_id%s' % n], '1', e.__str__())
                #         i = 0
                #     i = i + 1
                #     # arr_error = []
                #     try:
                #         # Создаем задачу на загрузку файлов
                #         globals()['task_%s' % i] = client.loop.create_task(element.download_media(folder))
                #         globals()['mess_id%s' % i] = element
                #         # task = client.loop.create_task(media.download_media(folder))
                #     except Exception as e:
                #         # Обработка исключения
                #         print(dialog.id, element.id, e.__str__())
                #         process_exeption(dialog.id, element.id, '1', e.__str__())
                # else:
                #     if i != 0:
                #         for n in range(1, i + 1):
                #             # download(dialog.id, globals()['task_%s' % n], globals()['mess_id%s' % i])
                #             try:
                #                 await globals()['task_%s' % n]
                #                 fn = globals()['mess_id%s' % i].audio.attributes[1].file_name
                #                 print(f'Complate {fn}')
                #             except Exception as e:
                #                 print(dialog.id, globals()['mess_id%s' % n], e.__str__())
                #                 process_exeption(dialog.id, globals()['mess_id%s' % n], '1', e.__str__())
                # update_last_date(dialog.id, dialog.date)

if __name__ == 'main':
    config = configparser.ConfigParser()
    config.read('config.ini')
    api_id = config['Telegram']['api_id']
    api_hash = config['Telegram']['api_hash']
    username = config['Telegram']['username']
    proxy_server = config['Telegram']['proxy_server']
    proxy_port = config['Telegram']['proxy_port']
    proxy_key = config['Telegram']['proxy_key']
    engine = create_engine(con_db.connect_string())
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    proxy = (socks.SOCKS5, proxy_server, proxy_port)
    client = TelegramClient(username, api_id, api_hash,
                            proxy=(socks.SOCKS5, '127.0.0.1', 9150)
                            )
    client.start()

    with client:
        client.loop.run_until_complete(main('music'))

    print('END')
