from sqlalchemy.sql import exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import and_, or_
from sqlalchemy import Column, String, BIGINT, Integer


class Music_db(declarative_base()):
    __tablename__ = 'music'
    trak_id = Column(BIGINT, primary_key=True)
    name = Column(String(100))
    artist = Column(String(100))
    path = Column(String(200))
    album = Column(String(100))
    year = Column(Integer)

    def __init__(self,
                 name: String,
                 artist: String,
                 path_tr: String,
                 album: String,
                 year: Integer):
        self.name = name
        self.artist = artist
        self.path = path_tr
        self.album = album
        self.year = year


def unique_record_music(element: dict, session) -> bool:
    # Проверка на уникальность ошибки
    result = session.query(exists().where(or_(and_(Music_db.name == element['name'].upper(),
                                                   Music_db.artist == element['artist'].upper()),
                                              Music_db.path == element['path']))).scalar()
    return not result


def new_record_music(element: dict, session):
    # Добавление в БД
    try:
        if unique_record_music(element, session):
            record = Music_db(element['name'], element['artist'], element['path'], element['album'], element['year'])
            session.add(record)
            session.commit()
    except Exception as e:
        session.rollback()
        print('dbError:', element['name'], element['artist'], e)


def get_atr(element: object, folder) -> dict:
    try:
        if element.audio.attributes[0].title is not None:
            el_name = element.audio.attributes[0].title.upper()
        else:
            el_name = element.audio.attributes[1].file_name.upper()
        if element.audio.attributes[0].performer is not None:
            el_artist = element.audio.attributes[0].performer.upper()
        else:
            el_artist = ''
        el_path = folder + element.audio.attributes[1].file_name
        el_album = 'NaN'
        el_year = element.audio.attributes[0].duration
        attribute = {'name': el_name,
                     'artist': el_artist,
                     'path': el_path,
                     'album': el_album,
                     'year': el_year}
    except Exception as e:
        print(e)
    return attribute
