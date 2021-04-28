import re
from sqlalchemy.sql import exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Text, DateTime
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker


class News_db(declarative_base()):
    __tablename__ = 'news'
    date = Column(DateTime, primary_key=True)
    description = Column(String(200), primary_key=True)
    link = Column(String(500))
    source = Column(String(200))
    info = Column(Text)

    def __init__(self,
                 date: DateTime,
                 desc: String,
                 link: String,
                 source: String,
                 info: Text):
        self.date = date
        self.description = desc
        self.link = link
        self.source = source
        self.info = info


def create_dict(date: DateTime, desc: str, link: str, source: int, info: str):
    attribute = {'date': date,
                 'desc': desc,
                 'link': link,
                 'source': source,
                 'info': info}
    return attribute


def unique_record(element: dict, session: sessionmaker, element_desc):
    # Проверка на уникальность
    # try:
        # element_desc = element['desc']
        # exists_emoji = demoji.findall(element_desc)
        # for emoji in exists_emoji:
        #     element_desc = re.sub(emoji, exists_emoji[emoji], element_desc)
    result = session.query(exists().where(and_(News_db.date == element['date'],
                                               News_db.description == element_desc
                                               ))).scalar()
    # except Exception as e:
    #     print(e)
    #     result = True
    return not result


async def new_record(element: dict, session, demoji, logging):
    # Добавление в БД
    try:
        element_desc = str(element['desc'])
        exists_emoji = demoji.findall(element_desc)
        for emoji in exists_emoji:
            element_desc = re.sub(emoji, exists_emoji[emoji], element_desc)
        if len(element_desc) > 500:
            element_desc_list = element_desc.split(' ')
            element_desc = ''
            for elment_list in element_desc_list:
                if len(element_desc) + len(elment_list) + 1 <= 500:
                    element_desc = element_desc.strip() + ' ' + elment_list
                else:
                    break
            element_desc = element_desc[:500]
        element_info = str(element['info'])
        exists_emoji = demoji.findall(element_info)
        for emoji in exists_emoji:
            element_info = re.sub(emoji, exists_emoji[emoji], element_info)

        if unique_record(element, session, element_desc):
            record = News_db(element['date'], element_desc, element['link'], element['source'], element_info)
            session.add(record)
            session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"New record news: {element['name']}, {e}")
        # print('dbError:', element['name'], e)


def get_atr_tlg(element: object, logging) -> dict:
    try:
        el_date = element.date
        el_desc = element.text
        el_link = 'NaN'
        el_source = element.chat.title + ' TLG'
        el_info = element.text
        attribute = create_dict(el_date, el_desc, el_link, el_source, el_info)
    except Exception as e:
        attribute = {'name': 'Error create attributes'}
        logging.error(f"get_atr news: {e}")
        # print(e)
    return attribute


# class News:
#     session = ''
#
#     def __init__(self, connection_string):
#         engine = create_engine(connection_string)
#         session_maker = sessionmaker(bind=engine)
#         self.session = session_maker()
#
#     class NewsDB(declarative_base()):
#         __tablename__ = 'news'
#         date = Column(Date, primary_key=True)
#         description = Column(String(200), primary_key=True)
#         link = Column(String(200))
#         source = Column(String(20))
#
#         def __init__(self, date, desc, ln, source):
#             self.date = date
#             self.description = desc
#             self.link = ln
#             self.source = source
#
#     def unique_record(self, new_date, new_desc):
#         result = self.session.query(exists().where(
#             self.NewsDB.description == new_desc)).scalar()
#         return result
#
#     def new_record(self, arr_news: object) -> object:
#         for element in arr_news:
#             if not self.unique_record(element['date'], element['desc']):
#                 record = self.NewsDB(element['date'], element['desc'], element['href'], element['source'])
#                 self.session.add(record)
#                 self.session.commit()
#             else:
#                 print(element['date'], element['desc'])
#
#     def get_records_by_date(self, date_from, date_to):
#         for instance in self.session.query(self.NewsDB).filter(self.NewsDB.date > date_from,
#                                                                self.NewsDB.date < date_to):
#             print(instance.date, instance.description, instance.link)