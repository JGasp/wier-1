import string
from typing import List, Dict
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from crawler.core.queue import TaskQueue
from crawler.core.task import WebPageCrawlTask, SiteMetadata
from crawler.core.job import WebCrawlJob
import multiprocessing
import threading

from crawler.database.datastore import PostgreSqlDataStore
from crawler.database.tables import Site


class TaskManager:

    def __init__(self):
        self.frontier = TaskQueue()
        self.jobs: List[WebCrawlJob] = []
        self.lock = threading.Lock()
        self.thread_sleeping = 0

        self.dataStore = PostgreSqlDataStore()
        self.sites: Dict[string, SiteMetadata] = {}

    def set_frontier(self, urls: []):
        for u in urls:
            self.frontier.add_item(WebPageCrawlTask(u, None))

    def start(self):
        number_of_threads = multiprocessing.cpu_count()

        for i in range(number_of_threads):
            self.jobs.append(WebCrawlJob(self))

    def get_next_frontier(self):
        with self.lock:
            return self.frontier.get_next()

    def get_domain(self, url):
        url_parsed = urlparse(url)
        domain = '%s://%s' % (url_parsed.schema, url_parsed.netloc)
        return domain

    def get_site_metadata(self, url):
        domain = self.get_domain(url)

        if domain in self.sites:
            return self.sites.get(domain)
        else:
            site = Site()
            site.domain = domain

            robot_url = '%s/robots.txt' % domain
            robot_url_res = requests.get(robot_url)
            rp = None
            if robot_url_res.status_code == 200:
                site.robots_content = robot_url_res.content
                rp = RobotFileParser()
                rp.parse(robot_url_res.content)

            sitemap_url = '%s/sitemap.xml' % domain
            site_map_res = requests.get(sitemap_url)

            sitemap_tags = []
            if site_map_res.status_code == 200:
                site.sitemap_content = site_map_res.content
                soup = BeautifulSoup(site_map_res.content)
                sitemap_tags = soup.find_all("sitemap")

            self.dataStore.persist(site)
            metadata = SiteMetadata(site, rp)
            self.sites[domain] = metadata

            for sm in sitemap_tags:
                sitemap_url = sm.findNext("loc").text
                if metadata.can_fetch(sitemap_url):
                    self.frontier.add_item(WebPageCrawlTask(sitemap_url, url, metadata))

            return metadata

    def add_new_page(self, frontier: WebPageCrawlTask):
        with self.lock:
            metadata = self.get_site_metadata(frontier.url)

            if metadata.can_fetch(frontier.url):
                frontier.metadata = metadata
                self.frontier.add_item(frontier)

                if self.thread_sleeping > 0:
                    self.wake_up_waiting_thread()

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

    def check_if_jobs_completed_and_frontier_empty(self):
        if self.frontier.queue.__len__() == 0:
            for job in self.jobs:
                if not job.event.is_set():
                    return False

            return True
        else:
            return False
