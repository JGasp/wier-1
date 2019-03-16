import string
from typing import List, Dict
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from crawler.core.queue import TaskQueue
from crawler.core.task import WebPageCrawlTask, SiteMetadata
import crawler.core.job as web_jobs
import multiprocessing
import threading

from crawler.database.datastore import PostgreSqlDataStore
from crawler.database.tables import Site


class TaskManager:

    def __init__(self, url_validation):
        self.initial_seed = []
        self.frontier = TaskQueue()
        self.jobs: List[web_jobs.WebCrawlJob] = []
        self.lock = threading.Lock()
        self.thread_sleeping = 0

        self.dataStore = PostgreSqlDataStore()
        self.sites: Dict[string, SiteMetadata] = {}

        self.visited_pages = {}
        self.website_visited__hashed_content = {}
        self.url_validation = url_validation

    def set_frontier(self, urls: []):
        self.initial_seed = urls
        for u in urls:
            self.add_new_page(WebPageCrawlTask(u))

    def start(self, num_of_jobs=0):
        if num_of_jobs == 0:
            num_of_jobs = multiprocessing.cpu_count()

        for i in range(num_of_jobs):
            job = web_jobs.WebCrawlJob(self, i)
            job.start()
            self.jobs.append(job)

        for j in self.jobs:
            j.join()

    def get_next_page(self):
        with self.lock:
            return self.frontier.get_next()

    def get_domain(self, url):
        url_parsed = urlparse(url)
        domain = '%s://%s' % (url_parsed.scheme, url_parsed.netloc)
        return domain

    def get_site_metadata(self, url):
        domain = self.get_domain(url)

        if domain in self.sites:
            return self.sites.get(domain)
        else:
            site = Site()
            site.domain = domain

            robot_url = '%s/robots.txt' % domain
            rp = None
            try:
                robot_url_res = requests.get(robot_url, verify=False, timeout=4)
                if robot_url_res.status_code == 200:
                    site.robots_content = str(robot_url_res.text)
                    rp = RobotFileParser()
                    rp.parse(site.robots_content)
            except Exception:
                pass

            sitemap_tags = []
            sitemap_url = '%s/sitemap.xml' % domain
            try:
                site_map_res = requests.get(sitemap_url, verify=False, timeout=4, )
                if site_map_res.status_code == 200:
                    if site_map_res.text.contains('<?xml version="1.0" encoding="UTF-8"?>'):
                        site.sitemap_content = str(site_map_res.text)
                        soup = BeautifulSoup(site.sitemap_content)
                        sitemap_tags = soup.find_all("sitemap")
            except Exception:
                pass

            site_id = self.dataStore.persist(site)
            metadata = SiteMetadata(site_id, rp)
            self.sites[domain] = metadata

            for sm in sitemap_tags:
                sitemap_url = sm.findNext("loc").text
                if metadata.can_fetch(sitemap_url):
                    self.frontier.add_item(WebPageCrawlTask(sitemap_url, metadata.db_site_id))

            return metadata

    def add_new_page(self, crawl_task: WebPageCrawlTask):
        with self.lock:
            if (self.url_validation(crawl_task.url) or self.is_valid_file(crawl_task.url)) and crawl_task.url not in self.visited_pages:
                metadata = self.get_site_metadata(crawl_task.url)

                if metadata.can_fetch(crawl_task.url):
                    self.visited_pages[crawl_task.url] = True

                    crawl_task.metadata = metadata
                    self.frontier.add_item(crawl_task)

                    if self.thread_sleeping > 0:
                        self.wake_up_waiting_thread()

    def is_valid_file(self, url):
        return url.endswith('.pdf') or url.endswith('.doc') or url.endswith('.docx') or url.endswith('.ppt') or url.endswith('.pptx')

    def is_valid_image(self, url):
        return url.endswith('.jpg') or url.endswith('.gif') or url.endswith('.png') or url.endswith('.jpeg') or url.endswith('.bmp') or url.endswith('.tiff') or url.endswith('.svg')

    def wake_up_waiting_thread(self):
        for job in self.jobs:
            if not job.event.is_set():
                job.event.set()
                self.thread_sleeping -= 1
                break

    def handle_waiting_thread(self):
        if self.check_if_jobs_completed_and_frontier_empty():
            print('Finished crawling web')
        else:
            with self.lock:
                self.thread_sleeping += 1

    def download_additional_content(self, url):
        for u in self.initial_seed:
            if url.startswith(u):
                return True
        return False

    def check_if_jobs_completed_and_frontier_empty(self):
        if self.frontier.queue.__len__() == 0:
            for job in self.jobs:
                if not job.event.is_set():
                    return False

            return True
        else:
            return False
