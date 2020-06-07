import configparser
import socks
import chardet
import con_db
import os
import proc_chat
import proc_error
import proc_music
import proc_films
import proc_book
from telethon.sync import TelegramClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# def download(dialog_id, mess, task):
#     try:
#         await globals()['task_%s' % n]
#         fn = globals()['mess_id%s' % n].audio.attributes[1].file_name
#         print(f'Complate {fn}')
#     except Exception as e:
#         print(dialog_id, globals()['mess_id%s' % n].id, e.__str__())
#         proc_error.process_exeption(dialog.id,
#                                     globals()['mess_id%s' % n].id,
#                                     '1',
#                                     e.__str__(),
#                                     session)


async def process_chat(dialog: object, folder, category):
    # Обработка чатов с музыкой
    msg_list = []
    lst_date = proc_chat.get_last_date(dialog.id, session)
    # list_chat_errors = get_chat_errors(dialog.id)
    # Сообщения которые мы не обрабатывали
    async for msg in client.iter_messages(dialog):
        if lst_date < msg.date.replace(tzinfo=None) \
                and ((category == 'music' and msg.audio)
                     or (category == 'book' and msg.document)
                     or (category == 'app' and msg.file)
                     or (category == 'film' and msg.video)):
            msg_list.append(msg)
    i = 0
    for element in msg_list:
        if category == 'music':
            element_atr = proc_music.get_atr(element, folder)
            await proc_music.new_record_music(element_atr, session)
        elif category == 'book':
            element_atr = proc_book.get_atr(element, folder)
            await proc_book.new_record(element_atr, session)
        elif category == 'app':
            pass
        elif category == 'films':
            pass
        else:
            pass
        # Определяет количество потоков загрузки
        if i >= 20:
            # Когда собрали нужное количество потоков, начинаем грузить
            for n in range(1, i + 1):
                try:
                    await globals()['task_%s' % n]
                    # if globals()['mess_id%s' % n].id in list_chat_errors:
                    #     delete_record_err(dialog.id, globals()['mess_id%s' % n].id)
                    # fn = globals()['mess_id%s' % n].audio.attributes[1].file_name
                    fn = globals()['class_%s' % n]['name']
                    print(f'Complate {fn}')
                except Exception as e:
                    print(dialog.id, globals()['mess_id%s' % n].id, '1', e.__str__())
                    await proc_error.process_exeption(dialog.id, globals()['mess_id%s' % n].id, '1', e.__str__(),
                                                      session)
            i = 0
        try:
            # Создаем задачу на загрузку файлов
            if not os.path.exists(element_atr['path']):
                i = i + 1
                globals()['task_%s' % i] = client.loop.create_task(element.download_media(folder))
                globals()['mess_id%s' % i] = element
                globals()['class_%s' % i] = element_atr
        except Exception as e:
            # Обработка исключения
            print(dialog.id, element.id, e.__str__())
            proc_error.process_exeption(dialog.id, element.id, '1', e.__str__(), session)
    else:
        if i != 0:
            for n in range(1, i + 1):
                # download(dialog.id)
                try:
                    await globals()['task_%s' % n]
                    # fn = globals()['mess_id%s' % n].audio.attributes[1].file_name
                    fn = globals()['class_%s' % n]['name']
                    print(f'Complate {fn}')
                except Exception as e:
                    print(dialog.id, globals()['mess_id%s' % n].id, e.__str__())
                    proc_error.process_exeption(dialog.id,
                                                globals()['mess_id%s' % n].id,
                                                '1',
                                                e.__str__(),
                                                session)
    proc_chat.update_last_date(dialog.id, dialog.date, session)


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
                # await process_chat(dialog, folder=music_path)  # , category='music')
            elif type_chat == 'film':
                # await proc_films.process_films(dialog)
                pass
            elif type_chat == 'book':
                await process_chat(dialog, folder='/home/aleksandr/Yandex.Disk/Book/', category='book')
                # pass
            elif type_chat == 'app':
                # await process_chat(dialog, folder='~/google-drive/app/', category='app')
                pass
            elif type_chat == 'people':
                # await process_chat(dialog, folder='/home/aleksandr/app/', category='people')
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
