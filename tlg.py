import configparser
import socks
import chardet
import con_db
import os
import proc_chat
import proc_error
import proc_music
# from os import path, remove
# from telethon import functions
from telethon.sync import TelegramClient
from sqlalchemy import create_engine  # , and_
# from sqlalchemy import Column, String, DateTime, BIGINT, Integer
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# from sqlalchemy.sql import exists, select
# from telethon.tl.types import PeerUser, PeerChat, PeerChannel


# from telethon import connection
# from detetime import date, datetime
# from telethon.tl.functions.channels import GetParticipantsRequest
# from telethon.tl.types import ChannelParticipantsSearch
# from telethon.tl.functions.messages import GerHistoryRequest


# class TlgChanel(declarative_base()):
#     # Класс канала
#     __tablename__ = 'tlg'
#     chat = Column(BIGINT, primary_key=True)
#     type = Column(String(20))
#     last_date = Column(DateTime)
#     name = Column(String(50))
#
#     def __init__(self, chat, type, last_date, name):
#         self.chat = chat
#         self.type = type
#         self.last_date = last_date
#         self.name = name


# class TlgError(declarative_base()):
#     # Класс для ошибок
#     __tablename__ = 'tlg_error'
#     chat = Column(BIGINT, primary_key=True)
#     message = Column(BIGINT, primary_key=True)
#     status = Column(Integer)
#     desc = Column(String(500))
#
#     def __init__(self, chat, message, status, desc):
#         self.chat = chat
#         self.message = message
#         self.status = status
#         self.desc = desc


# def unique_record():
#     # Проверка на уникальность ошибки
#     result = session.query(exists().where(and_(TlgError.chat == chat, TlgError.message == mess))).scalar()
#     return result


# def unique_record_err(chat: int, mess: int):
#     # Проверка на уникальность ошибки
#     result = session.query(exists().where(and_(TlgError.chat == chat, TlgError.message == mess))).scalar()
#     return result


# def delete_record_err(chat: int, mess: int):
#     try:
#         session.query(TlgError).filter(and_(TlgError.chat == chat, TlgError.message == mess)).delete()
#         session.commit()
#     except Exception as e:
#         print('Delet record error: ', chat, mess)
#         session.rollback()
#         print('dbError:', chat, e)


# def unique_record(chat: str):
#     # Проверка на уникальность чата
#     result = session.query(exists().where(TlgChanel.chat == chat)).scalar()
#     return result
#
#
# def get_last_date(chat: str) -> DateTime:
#     # Последнее просмотренное сообщение
#     result = session.query(TlgChanel).filter(TlgChanel.chat == chat)
#     return result[0].last_date
#
#
# def update_last_date(chat: str, new_last_date: DateTime):
#     try:
#         result = session.query(TlgChanel).filter(TlgChanel.chat == chat). \
#             update({TlgChanel.last_date: new_last_date}, synchronize_session=False)
#         session.commit()
#     except Exception as e:
#         session.rollback()
#         print('dbError:', chat, new_last_date, e)


# def get_status_mess(chat: int, mess_id: int) -> str:
#     # Получаем статус сообщения
#     result = session.query(TlgError).filter(and_(TlgError.chat == chat, TlgError.message == mess_id))
#     ret = 'OK'
#     try:
#         if result[0].status > 1:
#             ret = 'ERROR'
#     except Exception as e:
#         ret = 'OK'
#     return ret
#
#
# def get_chat_errors(chat: int) -> list:
#     # Получаем статус сообщения
#     result = session.query(TlgError).filter(TlgError.chat == chat)
#     list_err_mes = [row.message for row in result]
#     return list_err_mes


# def get_type(chat: str):
#     # Получаем тип чата
#     result = session.query(TlgChanel).filter(TlgChanel.chat == chat)
#     return result[0].type
#
#
# def new_record(element: object) -> object:
#     # Добавление чата
#     try:
#         record = TlgChanel(element['chat'], element['type'], element['last_date'], element['name'])
#         session.add(record)
#         session.commit()
#     except Exception as e:
#         session.rollback()
#         print('dbError:', element['chat'], element['name'], e)


# def process_exeption(id, msg, status, desc):
#     # Обработка исключений
#     ex_r = {'chat': id,
#             'message': msg,
#             'status': status,
#             'desc': desc}
#     new_record_err(ex_r)


def download(dialog_id, mess, task):
    try:
        task
        print(f'Complate {mess.audio.attributes[1].file_name}')
    except Exception as e:
        print(dialog_id, mess.id, '1', e.__str__())
        proc_error.process_exeption(dialog_id, mess.id, '1', e.__str__(), session)


async def process_music(dialog: object, folder, category):
    # Обработка чатов с музыкой
    msg_list = []
    lst_date = proc_chat.get_last_date(dialog.id, session)
    # list_chat_errors = get_chat_errors(dialog.id)
    async for msg in client.iter_messages(dialog):
        if lst_date < msg.date.replace(tzinfo=None) \
                and ((category == 'music' and msg.audio)
                     or (category == 'book' and msg.file)
                     or (category == 'app' and msg.file)
                     or (category == 'film' and msg.video)):
            msg_list.append(msg)
    i = 0

    for element in msg_list:
        # Определяет количество потоков загрузки
        if i >= 20:
            print(i)
            for n in range(1, i + 1):
                try:
                    await globals()['task_%s' % n]
                    # if globals()['mess_id%s' % n].id in list_chat_errors:
                    #     delete_record_err(dialog.id, globals()['mess_id%s' % n].id)
                    fn = globals()['mess_id%s' % n].audio.attributes[1].file_name
                    print(f'Complate {fn}')
                except Exception as e:
                    print(dialog.id, globals()['mess_id%s' % n].id, '1', e.__str__())
                    await proc_error.process_exeption(dialog.id, globals()['mess_id%s' % n].id, '1', e.__str__(),
                                                      session)
            i = 0
        # process_file = folder + element.document.attributes[1].file_name
        # if not path.exists(process_file) \
        #         or path.getsize(process_file) == 0:
        #     if path.exists(process_file) \
        #             and path.getsize(process_file) == 0:
        #         remove(process_file)
        #

        if category == 'music':
            music_atr = proc_music.get_atr(element, folder)

        try:
            # Создаем задачу на загрузку файлов
            proc_music.new_record_music(music_atr, session)
            if not os.path.exists(music_atr['path']):
                i = i + 1
                globals()['task_%s' % i] = client.loop.create_task(element.download_media(folder))
                globals()['mess_id%s' % i] = element
                globals()['class_%s' % i] = music_atr
        except Exception as e:
            # Обработка исключения
            print(dialog.id, element.id, e.__str__())
            proc_error.process_exeption(dialog.id, element.id, '1', e.__str__(), session)
    else:
        if i != 0:
            print(i)
            for n in range(1, i + 1):
                try:
                    await globals()['task_%s' % n]
                    fn = globals()['mess_id%s' % n].audio.attributes[1].file_name
                    print(f'Complate {fn}')
                except Exception as e:
                    print(dialog.id, globals()['mess_id%s' % n].id, e.__str__())
                    proc_error.process_exeption(dialog.id,
                                                      globals()['mess_id%s' % n].id,
                                                      '1',
                                                      e.__str__(),
                                                      session)
    proc_chat.update_last_date(dialog.id, dialog.date, session)


async def process_films(dialog: object):
    # Обработка чатов с видео
    msg_list = []
    lst_date = proc_chat.get_last_date(dialog.id, session)
    # list_chat_errors = get_chat_errors(dialog.id)
    async for msg in client.iter_messages(dialog):
        if lst_date < msg.date.replace(tzinfo=None) \
                and msg.video:
            msg_list.append(msg)


async def process_book(dialog: object):
    # Обработка чатов с видео
    msg_list = []
    lst_date = proc_chat.get_last_date(dialog.id, session)
    # list_chat_errors = get_chat_errors(dialog.id)
    async for msg in client.iter_messages(dialog):
        if lst_date < msg.date.replace(tzinfo=None) \
                and msg.file:
            msg_list.append(msg)


async def main():
    # для поиска ошибок
    # my_channel = await client.get_entity(PeerChannel(some_id))
    # list_chat_errors = get_chat_errors(dialog.id)
    # msg.id in list_chat_errors
    # or
    dialogs = await client.get_dialogs()  # Получаем список диалогов
    for dialog in dialogs:
        # Новый чат добавляем
        print('Process chat - ' + dialog.name)
        if not proc_chat.unique_record(dialog.id, session):
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
                             'last_date': '2000-01-01 00:00:00',  # dialog.date,
                             'name': mess_bytes.decode('UTF-8')}
                proc_chat.new_record(chat_info, session)
            else:
                # Если кодировка не определилась добавляем сообщение в ошибку
                chat_info = {'chat': dialog.id,
                             'type': '0',
                             'last_date': '2000-01-01 00:00:00',  # dialog.date,
                             'name': mess_bytes.decode('UTF-8')}
                proc_chat.new_record(chat_info, session)
                print(dialog.id, dialog.title)
        else:
            # Чат есть в списке пошли смотреть сообщения
            type_chat = proc_chat.get_type(dialog.id, session)
            if type_chat == 'music':
                pass
                if os.path.exists('/mnt/win/Users/budzi/OneDrive/Музыка/music'):
                    music_path = '/mnt/win/Users/budzi/OneDrive/Музыка/music/'
                else:
                    music_path = '/home/aleksandr/music/'
                    if not os.path.exists(music_path):
                        os.mkdir(music_path)
                await process_music(dialog, folder=music_path, category='music')
            elif type_chat == 'film':
                # await process_films(dialog)
                pass
            elif type_chat == 'book':
                # await process_music(dialog, folder=f'/home/aleksandr/book/', category='book')
                pass
            elif type_chat == 'app':
                # await process_music(dialog, folder=f'/home/aleksandr/app/', category='app')
                pass
            elif type_chat == 'people':
                # await process_book(dialog)
                # await process_music(dialog, folder=f'/home/aleksandr/app/', category='people')
                pass
            else:
                pass


if __name__ == '__main__':
    proxies = {
        'http': 'socks5://localhost:9050',
        'https': 'socks5://localhost:9050'
    }
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
                            proxy=(socks.SOCKS5, 'localhost', 9050)  # (socks.SOCKS5, '127.0.0.1', 9150)
                            )
    client.start()

    with client:
        client.loop.run_until_complete(main())
    print('END')
