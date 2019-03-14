from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from crawler.database.tables import Base
from crawler.database.tables import Site


class PostgreSqlDataStore:
    def __init__(self):
        self.engine = create_engine('postgresql+psycopg2://postgres:root@localhost/crawldb')
        self.Session = sessionmaker(bind=self.engine)
        self.session = None

    def start_session(self):
        self.session = self.Session()

    def persist(self, db_item: Base):
        self.session.add(db_item)

    def close(self):
        self.session.close()

    def commit(self):
        self.session.commit()

    def persist_test(self):
        self.start_session()
        site = Site(domain='asd', robots_content='-', sitemap_content='-')

        self.persist(site)
        self.commit()
        self.close()


