from sqlalchemy.sql import exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, BIGINT, Integer


class App_db(declarative_base()):
    __tablename__ = 'app'
    app_id = Column(BIGINT, primary_key=True)
    name = Column(String(100))
    developer = Column(String(100))
    path = Column(String(200))
    version = Column(String(30))
    language = Column(String(3))
    description = Column(String(1000))

    def __init__(self,
                 name: String,
                 developer: String,
                 path_tr: String,
                 version: Integer,
                 language: String,
                 description: String):
        self.name = name
        self.developer = developer
        self.path = path_tr
        self.version = version
        self.language = language
        self.description = description


def unique_record(element: dict, session) -> bool:
    # Проверка на уникальность
    result = session.query(exists().where(App_db.path == element['path'])).scalar()
    return not result


async def new_record(element: dict, session):
    # Добавление в БД
    try:
        if unique_record(element, session):
            record = App_db(element['name'],
                            element['developer'],
                            element['path'],
                            element['version'],
                            element['language'],
                            element['description'])
            session.add(record)
            session.commit()
        else:
            print('Dublicate file: {0}'.format(element['name']))
    except Exception as e:
        session.rollback()
        print('dbError:', element['name'], e)


def get_atr(element: object, folder) -> dict:
    try:
        if element.document.attributes[0].file_name is not None:
            el_name = element.document.attributes[0].file_name.upper()
        else:
            el_name = element.document.attributes[1].file_name.upper()
        # el_name = element.document.attributes[0].file_name.upper()
        el_developer = 'NaN'
        if element.document.attributes[0].file_name is not None:
            el_path = element.document.attributes[0].file_name.upper()
        else:
            el_path = element.document.attributes[1].file_name.upper()
        # el_path = folder + element.document.attributes[0].file_name
        el_version = 'NaN'
        el_language = 'NaN'
        el_description = 'NaN'
        attribute = create_dict(el_name, el_developer, el_path, el_version, el_language, el_description)
    except Exception as e:
        attribute = {'error': 'Error create attributes'}
        print(e)
    return attribute


def create_dict(in_name: str,
                in_developer: str,
                in_path: str,
                in_version: int,
                in_language: str,
                in_description: str):
    attribute = {'name': in_name,
                 'developer': in_developer,
                 'path': in_path,
                 'version': in_version,
                 'language': in_language,
                 'description': in_description}
    return attribute
