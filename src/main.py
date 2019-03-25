from urllib.parse import urlparse
import urllib3
import requests

from requests.adapters import HTTPAdapter

from crawler.core.manager import TaskManager

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

s = requests.Session()
s.mount('http://', HTTPAdapter(max_retries=1))
s.mount('https://', HTTPAdapter(max_retries=1))
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


NUMBER_OF_THREADS = 2

INITIAL_FRONTIER = [
    'https://evem.gov.si',
    'https://e-uprava.gov.si',
    'https://podatki.gov.si',
    'http://e-prostor.gov.si'
]

EXTENDED_FRONTIER = [
    'http://www.fu.gov.si',
    'http://www.mddsz.gov.si',
    'https://www.ess.gov.si'
    'http://www.osha.mddsz.gov.si',
    'http://www.upravneenote.gov.si'
]


def run_seed_domains_only_with_data_download():

    def is_url_valid_seed_crawl(url):
        domain = TaskManager.get_canonized_url(url)
        return domain == 'evem.gov.si' or domain == 'e-uprava.gov.si' or \
               domain == 'podatki.gov.si' or domain == 'e-prostor.gov.si'

    crawl_manager = TaskManager(is_url_valid_seed_crawl)
    crawl_manager.download_additional_content = True
    crawl_manager.data_store.clear_db()
    crawl_manager.set_frontier(INITIAL_FRONTIER)
    crawl_manager.start(num_of_jobs=NUMBER_OF_THREADS)


def run_gov_domains_only_without_data_download():

    urls = INITIAL_FRONTIER
    urls.extend(EXTENDED_FRONTIER)

    def is_url_valid(url):
        parsed_url = urlparse(url)
        return parsed_url.netloc.endswith('.gov.si')

    crawl_manager = TaskManager(is_url_valid)
    crawl_manager.data_store.clear_db()
    crawl_manager.set_frontier(urls)
    crawl_manager.start(num_of_jobs=NUMBER_OF_THREADS)


# run_seed_domains_only_with_data_download()
run_gov_domains_only_without_data_download()
