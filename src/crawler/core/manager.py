import hashlib
import string
from typing import List, Dict
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from crawler.core.queue import TaskQueue
from crawler.core.task import WebPageCrawlTask, SiteMetadata, WebPageCrawlResults
import crawler.core.job as web_jobs
import multiprocessing
import threading

from crawler.database.datastore import PostgreSqlDataStore
from crawler.database.tables import Site, Link


class TaskManager:

    def __init__(self, url_validation):
        self.initial_seed = []
        self.frontier = TaskQueue()
        self.jobs: List[web_jobs.WebCrawlJob] = []
        self.lock = threading.Lock()
        self.thread_sleeping = 0

        self.data_store = PostgreSqlDataStore()
        self.sites: Dict[string, SiteMetadata] = {}

        self.visited_pages = {}
        self.website_visited_hashed_content = {}
        self.url_validation = url_validation

    def start(self, num_of_jobs=0):
        if num_of_jobs == 0:
            num_of_jobs = multiprocessing.cpu_count()

        for i in range(num_of_jobs):
            job = web_jobs.WebCrawlJob(self)
            job.start()
            self.jobs.append(job)

        for j in self.jobs:
            j.join()

    def set_frontier(self, urls: []):
        self.initial_seed = urls
        web_tasks = []
        for u in urls:
            web_tasks.append(WebPageCrawlTask(u))

        self.add_new_web_tasks(web_tasks, None, None, 0)

    def handle_crawl_results(self, crawl_results: WebPageCrawlResults, crawl_task: WebPageCrawlTask):
        with self.lock:
            page = crawl_results.page

            page_id = None
            if crawl_results.page.page_type_code == 'HTML':
                hash_code = None
                if page.html_content is not None:
                    hash_code = hashlib.sha256(page.html_content.encode('utf-8')).digest()

                if hash_code is not None and hash_code in self.website_visited_hashed_content:
                    page_id_duplicate = self.website_visited_hashed_content[hash_code]
                    page.page_type_code = 'DUPLICATE'
                    page.html_content = None
                    page_id = self.data_store.persist(page)

                    if page_id_duplicate != crawl_task.from_page_id:
                        link = Link()
                        link.from_page = page_id_duplicate
                        link.to_page = page_id
                        self.data_store.persist(link)
                else:
                    page_id = self.data_store.persist(page)
                    self.website_visited_hashed_content[hash_code] = page_id

                    for i in crawl_results.images:
                        i.page_id = page_id
                        self.data_store.persist(i)

                    if crawl_task.site_map_crawl_tasks is not None:
                        for ct in crawl_task.site_map_crawl_tasks:
                            crawl_results.new_crawl_tasks.append(ct)

                    self.add_new_web_tasks(crawl_results.new_crawl_tasks, page_id, crawl_task.url, crawl_task.id_number)

            elif crawl_results.page.page_type_code == 'BINARY':
                page_id = self.data_store.persist(page)
                page_data = crawl_results.page_data
                page_data.page_id = page_id
                self.data_store.persist(page_data)

            if crawl_task.from_page_id is not None:
                link = Link()
                link.from_page = crawl_task.from_page_id
                link.to_page = page_id

                self.data_store.persist(link)

    @staticmethod
    def get_canonized_url(url, include_path=False, include_schema=False):
        url_parsed = urlparse(url)

        domain = url_parsed.netloc

        if include_path:
            domain += url_parsed.path

        if include_schema:
            domain = url_parsed.scheme + '://' + domain

        if domain[-1] == '/':
            domain = domain[:-1]

        return domain

    def get_site_metadata(self, url):
        domain = self.get_canonized_url(url)

        if domain in self.sites:
            return self.sites.get(domain), None
        else:
            site = Site()
            site.domain = domain

            robot_url = 'http://%s/robots.txt' % domain
            sitemap_url = None
            crawl_delay = None

            rp = None
            try:
                robot_url_res = requests.get(robot_url, verify=False, timeout=4)
                if robot_url_res.status_code == 200:
                    site.robots_content = str(robot_url_res.text)

                    rp = RobotFileParser()
                    rp.parse(site.robots_content)

                    r_lines = site.robots_content.split('\n')
                    for l in r_lines:
                        l_lower = l.lower()
                        if 'sitemap:' in l_lower:
                            sitemap_url = l[l_lower.find('sitemap:') + 9:]
                        if 'crawl-delay:' in l_lower:
                            crawl_delay = int(l[l_lower.find('crawl-delay:') + 13:])
            except Exception:
                pass

            sitemap_tags = []
            if sitemap_url is not None:
                try:
                    site_map_res = requests.get(sitemap_url, verify=False, timeout=4)
                    if site_map_res.status_code == 200:
                        site.sitemap_content = str(site_map_res.text)
                        soup = BeautifulSoup(site.sitemap_content)
                        sitemap_tags = soup.find_all("loc")
                except Exception:
                    pass

            site_id = self.data_store.persist(site)
            site_metadata = SiteMetadata(site_id, rp, crawl_delay)
            self.sites[domain] = site_metadata

            site_map_web_tasks = None
            for sm in sitemap_tags:
                sitemap_url = sm.text
                site_map_web_tasks = []
                if site_metadata.can_fetch(sitemap_url):
                    web_task = WebPageCrawlTask(sitemap_url)
                    self.update_web_crawl_task(web_task, site_metadata)
                    site_map_web_tasks.append(web_task)

            robot_info = ('' if rp is None else '[%s]' % robot_url)
            sitemap_info = ('' if sitemap_tags.__len__() == 0 else '[%s]' % sitemap_url)

            print('> [Job %s] Created entry for site [%s] %s %s' % (threading.get_ident(), domain, robot_info, sitemap_info))

            return site_metadata, site_map_web_tasks

    def update_web_crawl_task(self, crawl_task, metadata):
        crawl_task.site_id = metadata.site_id
        crawl_task.crawl_at_time = metadata.get_next_crawl_available_in()
        crawl_task.download_additional_content = self.download_additional_content(crawl_task.url)

    def build_new_task(self, crawl_task: WebPageCrawlTask):
        cannon_url = self.get_canonized_url(crawl_task.url, include_path=True)
        if cannon_url not in self.visited_pages:
            if self.is_valid_file(crawl_task.url):
                crawl_task.type = 'BINARY'
                crawl_task.download_additional_content = self.download_additional_content(crawl_task.from_page_url)

                if not crawl_task.download_additional_content:
                    return None
                else:
                    return crawl_task

            elif self.url_validation(crawl_task.url):
                site_metadata, site_map_crawl_tasks = self.get_site_metadata(crawl_task.url)

                if site_metadata.can_fetch(crawl_task.url):
                    self.visited_pages[cannon_url] = True

                    if site_map_crawl_tasks is not None:
                        crawl_task.site_map_crawl_tasks = site_map_crawl_tasks

                    crawl_task.site_id = site_metadata.site_id
                    crawl_task.crawl_at_time = site_metadata.get_next_crawl_available_in()
                    crawl_task.download_additional_content = self.download_additional_content(crawl_task.url)

                    return crawl_task
        else:
            return None

    def add_new_web_tasks(self, crawl_tasks: List[WebPageCrawlTask], from_page_id, from_page_url, from_task_id):
        new_crawl_tasks = []
        for ct in crawl_tasks:
            ct.from_page_id = from_page_id
            ct.from_page_url = from_page_url
            ct = self.build_new_task(ct)

            if ct is not None:
                new_crawl_tasks.append(ct)

        self.frontier.add_items(new_crawl_tasks, from_task_id)

        if self.thread_sleeping > 0:
            self.wake_up_waiting_thread(new_crawl_tasks.__len__())

    def wake_up_waiting_thread(self, count=1):
        for job in self.jobs:
            if not job.event.is_set():
                job.event.set()
                self.thread_sleeping -= 1
                count -= 1
                if count == 0:
                    break

    def handle_waiting_thread(self):
        if self.check_if_jobs_completed_and_frontier_empty():
            print('Finished crawling web')
        else:
            with self.lock:
                self.thread_sleeping += 1

    def download_additional_content(self, url):
        if url is not None:
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

    @staticmethod
    def is_valid_file(url):
        return url.endswith('.pdf') or url.endswith('.doc') or url.endswith('.docx') or url.endswith(
            '.ppt') or url.endswith('.pptx')

    @staticmethod
    def is_valid_image(url):
        return url.endswith('.jpg') or url.endswith('.gif') or url.endswith('.png') or url.endswith(
            '.jpeg') or url.endswith('.bmp') or url.endswith('.tiff') or url.endswith('.svg')
