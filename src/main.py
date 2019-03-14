from urllib.parse import urlparse
from crawler.database.datastore import PostgreSqlDataStore

initial_frontier = [
    'evem.gov.si',
    'e-uprava.gov.si',
    'podatki.gov.si',
    'e-prostor.gov.si'
]

# TODO add 5 additional .gov.si


def is_url_valid(url):
    parsed_url = urlparse(url)
    parsed_url.netloc.contaions('.gov.si')


# crawlManager = TaskManager()
# crawlManager.set_frontier(initial_frontier)
# crawlManager.start()

db = PostgreSqlDataStore()
db.persist_test()
