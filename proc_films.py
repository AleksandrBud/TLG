from sqlalchemy.sql import exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import and_, or_
from sqlalchemy import Column, String, BIGINT, Integer, Text


class Films_db(declarative_base()):
    __tablename__ = 'films'
    film_id = Column(BIGINT, primary_key=True)
    name = Column(String(100))
    description = Column(Text)
    link = Column(String(300))
    path = Column(String(200))
    year = Column(Integer)
    poster = Column(String(200))
    trailer = Column(String(200))

    def __init__(self,
                 name: String,
                 description: String,
                 link: String,
                 path: String,
                 year: Integer,
                 poster: String,
                 trailer: String):
        self.name = name
        self.description = description
        self.link = link
        self.path = path
        self.year = year
        self.poster = poster
        self.trailer = trailer


def unique_record(element: dict, session) -> bool:
    # Проверка на уникальность ошибки
    result = session.query(exists().where(or_(and_(Films_db.link == element['link'],
                                                   len(element['link']) > 0),
                                              and_(Films_db.path == element['path'],
                                                   len(element['path']) > 0)
                                              )
                                          )
                           ).scalar()
    return not result


async def new_record(element: dict, session):
    # Добавление в БД
    try:
        if unique_record(element, session):
            record = Films_db(element['name'],
                              element['description'],
                              element['link'],
                              element['path'],
                              element['year'],
                              element['poster'],
                              element['trailer'])
            session.add(record)
            session.commit()
        else:
            print('Dublicate file: {0}'.format(element['name']))
    except Exception as e:
        session.rollback()
        print('dbError:', element['name'], e)


def get_atr(element: object, folder) -> dict:
    try:
        if element.video is not None:
            long_name = element.message.split('\n')[0]
            el_name = long_name.split(' ')[0]
            el_description = 'NaN'
            el_path = folder + element.video.attributes[1].file_name
            el_link = 'NaN'
            el_year = int(long_name.translate(None, '()'))
            el_poster = 'NaN'
            el_trailer = 'NaN'
            attribute = create_dict(el_name, el_description, el_path, el_link, el_year, el_poster, el_trailer)
        elif element.buttons is not None:
            i: int = 0
            for button in element.buttons[0]:
                if button.text.contains('Смотреть'):
                    if len(element.buttons[0]) == 1:
                        long_name = element.message.split('\n')[0]
                        el_name = long_name.split(' ')[0]
                        el_year = int(long_name.split(' ')[1][1:5])
                        el_description = element.message.split('\n')[1]
                    else:
                        long_name = element.message.split('\n\n')[1]
                        list_films = long_name.split('\n')
                        el_name = list_films[i].replace(' СМОТРЕТЬ', '').replace(f'{i}. ', '')
                        el_description = 'NaN'
                        el_year = 0
                    el_path = 'NaN'
                    el_link = button.url
                    el_poster = element.web_preview.url
                    el_trailer = 'NaN'
                    attribute = create_dict(el_name, el_description, el_path, el_link, el_year, el_poster, el_trailer)
                    i = i + 1
    except Exception as e:
        attribute = 'error'
        print(e)
    return attribute


def create_dict(in_name: str,
                in_description: str,
                in_path: str,
                in_link: str,
                in_year: int,
                in_poster: str,
                in_trailer: str):
    attribute = {'name': in_name,
                 'description': in_description,
                 'path': in_path,
                 'link': in_link,
                 'year': in_year,
                 'poster': in_poster,
                 'trailer': in_trailer}
    return attribute
