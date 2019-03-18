from urllib.parse import urlparse
import urllib3
import requests

from requests.adapters import HTTPAdapter

from crawler.core.manager import TaskManager


s = requests.Session()
s.mount('http://', HTTPAdapter(max_retries=1))
s.mount('https://', HTTPAdapter(max_retries=1))

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


initial_frontier = [
    'https://evem.gov.si',
    'https://e-uprava.gov.si',
    'https://podatki.gov.si',
    'http://e-prostor.gov.si'
]

# TODO add 5 additional .gov.si


def is_url_valid(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc.endswith('.gov.si')


crawlManager = TaskManager(is_url_valid)
crawlManager.dataStore.clear_db()
crawlManager.set_frontier(initial_frontier)
crawlManager.start(num_of_jobs=2)

