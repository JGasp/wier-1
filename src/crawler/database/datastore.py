from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker


from crawler.database.tables import Base


class PostgreSqlDataStore:
    def __init__(self):
        self.engine = create_engine('postgresql+psycopg2://postgres:root@localhost/crawldb')
        self.Session = sessionmaker(bind=self.engine)

    def persist(self, db_item: Base):
        session = self.Session()
        session.add(db_item)
        session.commit()

        db_id = None
        if hasattr(db_item, 'id'):
            db_id = db_item.id

        session.close()

        return db_id

    def clear_db(self):
        sql = text('DELETE FROM crawldb.link; DELETE FROM crawldb.image; DELETE FROM crawldb.page_data; DELETE FROM crawldb.page; DELETE FROM crawldb.site;')
        result = self.engine.execute(sql)
