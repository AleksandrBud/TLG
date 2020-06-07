from sqlalchemy.sql import exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, BIGINT, Integer


class Book_db(declarative_base()):
    __tablename__ = 'book'
    book_id = Column(BIGINT, primary_key=True)
    name = Column(String(100))
    author = Column(String(100))
    path = Column(String(200))
    year = Column(Integer)
    language = Column(String(3))

    def __init__(self,
                 name: String,
                 author: String,
                 path_tr: String,
                 year: Integer,
                 language: String):
        self.name = name
        self.author = author
        self.path = path_tr
        self.year = year
        self.language = language


def unique_record(element: dict, session) -> bool:
    # Проверка на уникальность
    result = session.query(exists().where(Book_db.path == element['path'])).scalar()
    return not result


async def new_record(element: dict, session):
    # Добавление в БД
    try:
        if unique_record(element, session):
            record = Book_db(element['name'], element['author'], element['path'], element['year'], element['language'])
            session.add(record)
            session.commit()
        else:
            print('Dublicate file: {0}'.format(element['name']))
    except Exception as e:
        session.rollback()
        print('dbError:', element['name'], e)


def get_atr(element: object, folder) -> dict:
    try:
        el_name = element.document.attributes[0].file_name.upper()
        el_author = 'NaN'
        el_path = folder + element.document.attributes[0].file_name
        el_year = 0
        el_language = 'NaN'
        attribute = create_dict(el_name, el_author, el_path, el_year, el_language)
    except Exception as e:
        attribute = {'error': 'Error create attributes'}
        print(e)
    return attribute


def create_dict(in_name: str, in_author: str, in_path: str, in_year: int, in_language: str):
    attribute = {'name': in_name,
                 'author': in_author,
                 'path': in_path,
                 'year': in_year,
                 'language': in_language}
    return attribute
