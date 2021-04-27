import configparser
import socks
import chardet
import con_db
import os
import proc_chat
import proc_error
import proc_music
import proc_app
import proc_films
import proc_book
import proc_news
import demoji
import re
import logging
from telethon.sync import TelegramClient
from telethon import events
    # , functions
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from time import strftime


async def get_messages_from_chat(dialog: object, lst_date, category: str):
    msg_list = []
    async for msg in client.iter_messages(dialog):
        if lst_date < msg.date.replace(tzinfo=None) \
                and ((category == 'music' and msg.audio)
                     or (category == 'book' and msg.document)
                     or (category == 'app' and msg.document and not msg.video)
                     or (category == 'news')
                     or (category == 'film' and (msg.video or msg.buttons))):
            msg_list.append(msg)
    return msg_list


async def parce_maessages(msg_list: list, dialog_id: int, category: str, folder: str):
    i = 0
    for element in msg_list:
        download = True
        if category == 'music':
            element_atr = proc_music.get_atr(element, folder)
            await proc_music.new_record_music(element_atr, session)
        elif category == 'book':
            element_atr = proc_book.get_atr(element, folder)
            await proc_book.new_record(element_atr, session)
        elif category == 'app':
            element_atr = proc_app.get_atr(element, folder)
            await proc_app.new_record(element_atr, session)
        elif category == 'news':
            download = False
            element_atr = proc_news.get_atr_tlg(element)
            await proc_news.new_record(element_atr, session, demoji)
        elif category == 'film':
            element_atr = proc_films.get_atr(element, folder)
            await proc_films.new_record(element_atr, session)
            if not element.vido:
                download = False
        else:
            pass
        # Определяет количество потоков загрузки
        if download:
            if i >= 20:
                # Когда собрали нужное количество потоков, начинаем грузить
                for n in range(1, i + 1):
                    try:
                        await globals()['task_%s' % n]
                        fn = globals()['class_%s' % n]['name']
                        curr_date = strftime('[%Y-%b-%d %H:%M:%S]')
                        # logger.info(f'{curr_date} Complate {fn}')
                        print(f'Complate {fn}')
                    except Exception as e:
                        curr_date = strftime('[%Y-%b-%d %H:%M:%S]')
                        # logger.error(f"{curr_date} ERROR {dialog_id} {globals()['mess_id%s' % n].id}, {e.__str__()}")
                        print(dialog_id, globals()['mess_id%s' % n].id, '1', e.__str__())
                        # await proc_error.process_exeption(dialog_id, globals()['mess_id%s' % n].id, '1', e.__str__(),
                        #                                   session)
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
                print(dialog_id, element.id, e.__str__())
                proc_error.process_exeption(dialog_id, element.id, '1', e.__str__(), session)
    else:
        if download:
            if i != 0:
                for n in range(1, i + 1):
                    try:
                        await globals()['task_%s' % n]
                        # fn = globals()['mess_id%s' % n].audio.attributes[1].file_name
                        fn = globals()['class_%s' % n]['name']
                        print(f'Complate {fn}')
                    except Exception as e:
                        print(dialog_id, globals()['mess_id%s' % n].id, e.__str__())
                        proc_error.process_exeption(dialog_id,
                                                    globals()['mess_id%s' % n].id,
                                                    '1',
                                                    e.__str__(),
                                                    session)


async def process_chat(dialog: object, folder, category):
    # Обработка чатов с музыкой
    lst_date = proc_chat.get_last_date(dialog.id, session)
    # Сообщения которые мы не обрабатывали
    msg_list = await get_messages_from_chat(dialog, lst_date, category)
    await parce_maessages(msg_list, dialog.id, category, folder)
    proc_chat.update_last_date(dialog.id, dialog.date, session)


async def add_new_chat(dialog: object):
    chat_title = dialog.title
    exists_emoji = demoji.findall(chat_title)
    for emoji in exists_emoji:
        chat_title = re.sub(emoji, exists_emoji[emoji], chat_title)
    if len(chat_title) > 50:
        chat_title = dialog.title
        exists_emoji = demoji.findall(chat_title)
        for emoji in exists_emoji:
            chat_title = re.sub(emoji, '', chat_title)
    mess_bytes = chat_title.encode()
    code = chardet.detect(mess_bytes)
    det = chardet.universaldetector.UniversalDetector()
    det.feed(mess_bytes)
    if code['encoding'] is not None:
        # Если удалось определить кодировку сообщения, добавляем запись
        chat_info = {'chat': dialog.id,
                     'type': '0',
                     'last_date': '2000-01-01 00:00:00',
                     'name': mess_bytes.decode('UTF-8')}
        proc_chat.new_record(chat_info, session)
    else:
        # Если кодировка не определилась добавляем сообщение в ошибку
        chat_info = {'chat': dialog.id,
                     'type': '0',
                     'last_date': '2000-01-01 00:00:00',
                     'name': mess_bytes.decode('UTF-8')}
        proc_chat.new_record(chat_info, session)
        print(dialog.id, dialog.title)

async def main():
    dialogs = await client.get_dialogs()  # Получаем список диалогов
    for dialog in dialogs:
        # Новый чат добавляем
        print('Process chat - ' + dialog.name)
        if not proc_chat.unique_record(dialog.id, session):
            # Если диалога нет в нашей базе добавим его туда
            print('Add chat to list')
            await add_new_chat(dialog)
        else:
            # Чат есть в списке пошли смотреть сообщения
            type_chat = proc_chat.get_type(dialog.id, session)
            # proc_list = ['music', 'book', 'app']
            proc_list = ['news', 'music', 'book']
            if type_chat in proc_list:
                main_path = '/home/aleksandr/Загрузки/'
                path_for_save = main_path + type_chat + '/'
                if not os.path.exists(path_for_save):
                    os.mkdir(path_for_save)
                await process_chat(dialog, folder=path_for_save, category=type_chat)


if __name__ == '__main__':
    proxies = {
        'http': 'socks5://localhost:9050',
        'https': 'socks5://localhost:9050'
    }
    config = configparser.ConfigParser()
    config.read('/home/aleksandr/Yandex.Disk/github/TLG/config.ini')
    api_id = config['Telegram']['api_id']
    api_hash = config['Telegram']['api_hash']
    username = config['Telegram']['username']
    proxy_server = config['Telegram']['proxy_server']
    proxy_port = config['Telegram']['proxy_port']
    proxy_key = config['Telegram']['proxy_key']
    main_path = '/home/aleksandr/Загрузки/'
    logging.FileHandler
    # handler = logging.handlers.RotatingFileHandler('error.log',
    #                                                maxBytes=500000,
    #                                                backupCount=5)
    # logger = logging.getLogger(__name__)
    # logger.setLevel(logging.INFO)
    # logger.addHandler(handler)

    demoji.download_codes()
    engine = create_engine(con_db.connect_string())
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    proxy = (socks.SOCKS5, proxy_server, proxy_port)
    client = TelegramClient(username, api_id, api_hash,
                           # proxy=(socks.SOCKS5, 'localhost', 9050)  # (socks.SOCKS5, '127.0.0.1', 9150)
                            )

    mode_run_program = input('Choice mode run program (event(press e)/last messages(press l)): ')
    if mode_run_program == 'e':
        @client.on(events.NewMessage())
        async def normal_handler(event):
            #    print(event.message)
            # print(event.message.to_dict()['message'])
            chat_id = event.chat_id
            if proc_chat.unique_record(chat_id, session):
                type_chat = proc_chat.get_type(chat_id, session)
                proc_list = ['news', 'music', 'book']
                if type_chat in proc_list:
                    msg_list = [event.message]
                    path_for_save = main_path + type_chat + '/'
                    await parce_maessages(msg_list, chat_id, type_chat, path_for_save)
                    try:
                       await event.message.mark_read()
                    except Exception as e:
                        print(e)


        client.start()
        client.run_until_disconnected()
    elif mode_run_program == 'l':
        with client:
            client.loop.run_until_complete(main())
        print('END')
    else:
        print('Input mode not found!')
