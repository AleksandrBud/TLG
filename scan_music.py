import con_db
import asyncio
import os
import proc_music
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


async def process_dir(input_path):
    list_file = os.listdir(input_path)
    for file in list_file:
        abs_path = input_path + '/' + file
        if os.path.isdir(abs_path):
            # Входим в рекурсию
            await process_dir(abs_path)
        else:
            # обрабатываем файл
            await process_file(file, abs_path)


async def process_file(input_file: str, input_path: str):
    file_info = proc_music.create_dict(input_file, '', input_path, '', 0)
    await proc_music.new_record_music(file_info, session)


engine = create_engine(con_db.connect_string())
session_maker = sessionmaker(bind=engine)
session = session_maker()
main_path = '/mnt/win/Users/budzi/OneDrive/Музыка'
if os.path.exists(main_path):
    asyncio.run(process_dir(main_path))
