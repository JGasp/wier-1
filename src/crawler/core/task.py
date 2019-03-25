import time


class SiteMetadata:

    def __init__(self, site_id, rp, crawl_delay):
        self.site_id = site_id
        self.rp = rp

        self.crawl_delay = crawl_delay
        self.next_crawl_available_at = 0

    def can_fetch(self, url):
        if self.rp is not None:
            return self.rp.can_fetch("*", url)
        else:
            return True

    def get_next_crawl_available_in(self):
        if self.crawl_delay is not None:
            current_time = self.next_crawl_available_at
            self.next_crawl_available_at = time.time() + self.crawl_delay
            return current_time
        else:
            return None


class WebPageCrawlTask:
    def __init__(self, url, site_id=None, from_page_id=None, from_page_url=None, crawl_at_time=None):
        self.id_number = 0

        self.url = url
        self.site_id = site_id

        self.from_page_id = from_page_id
        self.from_page_url = from_page_url

        self.crawl_at_time = crawl_at_time

        self.site_map_crawl_tasks = None

        self.type = 'HTML'


class WebPageCrawlResults:

    def __init__(self, id_number):
        self.id_number = id_number
        self.crawl_task = None
        self.page = None
        self.page_data = None
        self.id_number = id_number
        self.new_crawl_tasks = []
        self.images = []

