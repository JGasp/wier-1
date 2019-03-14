from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, TIMESTAMP, Binary, Text

Base = declarative_base()
Base.metadata.schema = 'crawldb'


class DataType(Base):
    __tablename__ = 'data_type'
    id = Column(Integer, primary_key=True)
    code = Column(String(20), nullable=False)


class PageType(Base):
    __tablename__ = 'page_type'
    id = Column(Integer, primary_key=True)
    code = Column(String(20), nullable=False)


class Site(Base):
    __tablename__ = 'site'

    id = Column(Integer, primary_key=True)
    domain = Column(String(500))
    robots_content = Column(Text)
    sitemap_content = Column(Text)


class Page(Base):
    __tablename__ = 'page'

    id = Column(Integer, primary_key=True)
    site_id = Column(Integer, ForeignKey('site.id'))
    page_type_code = Column(String(20))
    url = Column(String(3000), unique=True)
    html_content = Column(Text)
    html_status_code = Column(Integer)
    accessed_time = Column(TIMESTAMP)


class PageData(Base):
    __tablename__ = 'page_data'

    id = Column(Integer, primary_key=True)
    page_id = Column(Integer, ForeignKey('page.id'))
    data_type_code = Column(String(20))
    data = Column(Binary)


class Image(Base):
    __tablename__ = 'image'

    id = Column(Integer, primary_key=True)
    page_id = Column(Integer, ForeignKey('image.id'))
    filename = Column(String(255))
    content_type = Column(String(50))
    data = Column(Binary)
    accessed_time = Column(TIMESTAMP)


class Link(Base):
    __tablename__ = 'link'

    id = Column(Integer, primary_key=True)
    from_page = Column(Integer, ForeignKey('page.id'))
    to_page = Column(Integer, ForeignKey('page.id'))
