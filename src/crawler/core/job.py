import threading
import time
from urllib.parse import urlparse

import requests
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options

from selenium import webdriver

from crawler.core.task import WebPageCrawlTask, WebPageCrawlResults
from crawler.database.tables import Page, Link, Image, PageData


class WebCrawlJob(threading.Thread):

    def __init__(self, manager):
        super(WebCrawlJob, self).__init__(target=self.execute_task, args=())

        self.is_running = True
        self.event = threading.Event()
        self.manager = manager

        self.web_driver = self.build_web_driver()

    def build_web_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--dns-prefetch-disable")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--ignore-urlfetcher-cert-requests")

        web_driver = webdriver.Chrome('C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe',
                                           options=options)
        web_driver.set_page_load_timeout(10)

        return web_driver

    def execute_task(self):
        while self.is_running:
            self.crawl_web()

    def stop_running(self):
        self.is_running = False
        self.web_driver.close()

    def crawl_web(self):
        crawl_task: WebPageCrawlTask = self.manager.frontier.get_next()

        if crawl_task is None:
            self.manager.handle_waiting_thread()
            print('[Job %s] Waiting' % threading.get_ident())
            self.event.wait()
            print('[Job %s] Continue' % threading.get_ident())
        else:
            crawl_results = WebPageCrawlResults(crawl_task.id_number)

            if crawl_task.crawl_at_time is not None:
                crawl_in = crawl_task.crawl_at_time - time.time()
                if crawl_in > 0:
                    print('[Job %d] Will crawl [%s] in %d s' % (threading.get_ident(), crawl_task.url, crawl_in))
                    time.sleep(crawl_in)

            print('[Job %d] Started crawling [%s]' % (threading.get_ident(), crawl_task.url))

            crawl_results.page = Page()
            crawl_results.page.url = self.manager.get_canonized_url(crawl_task.url, include_path=True)

            try:
                r = requests.get(crawl_task.url, verify=False)
                crawl_results.page.http_status_code = r.status_code
            except Exception:
                pass

            if crawl_task.type == 'BINARY':
                self.download_binary_file(crawl_results, crawl_task)
            else:
                self.process_html_page(crawl_results, crawl_task)

            self.manager.handle_crawl_results(crawl_results, crawl_task)

    @staticmethod
    def download_binary_file(crawl_results, crawl_task):
        crawl_results.page.page_type_code = 'BINARY'

        if crawl_task.download_additional_content:
            crawl_results.page_data = PageData()
            crawl_results.page_data.data_type_code = crawl_task.url[crawl_task.url.rfind('.'):]

            r = requests.get(crawl_task.url, allow_redirects=True, verify=False)
            # open('test', 'wb').write(r.content)

            crawl_results.page_data.data = r.content

    def process_html_page(self, crawl_results, crawl_task):
        crawl_results.page.page_type_code = 'HTML'

        try:
            self.web_driver.get(crawl_task.url)  # TODO handle redirects
        except TimeoutException:
            self.web_driver = self.build_web_driver()
            print('# [Job %d] Timeout on [%s]' % (threading.get_ident(), crawl_task.url))
            return

        crawl_results.page.html_content = self.web_driver.page_source

        self.parse_links(crawl_results, crawl_task)
        self.parse_and_download_images(crawl_results, crawl_task)

    def add_link(self, href, crawl_results, crawl_task):
        if href is not None and len(href) > 0 and 'javascript:' not in href and 'mailto:' not in href:
            if href[0:4] != 'http':
                href = crawl_task.url + '/' + href

            crawl_results.new_crawl_tasks.append(WebPageCrawlTask(href))

    def parse_links(self, crawl_results, crawl_task):
        hyperlinks = self.web_driver.find_elements_by_tag_name('a')
        for l in hyperlinks:
            try:
                href = l.get_attribute('href')
                self.add_link(href, crawl_results, crawl_task)
            except StaleElementReferenceException:
                pass

        click_events = self.web_driver.find_elements_by_xpath("//*[@onclick]")
        for ce in click_events:
            try:
                click_event = ce.get_attribute('onclick')

                if 'document.location' in click_event or 'location.href' in click_event:
                    url = click_event.split('=')[1]
                    self.add_link(url, crawl_results, crawl_task)
            except StaleElementReferenceException:
                pass

    def parse_and_download_images(self, crawl_results, crawl_task):
        images_src = []
        if crawl_task.download_additional_content:
            try:
                images = self.web_driver.find_elements_by_tag_name('img')
                for i in images:
                    src = i.get_attribute('src')

                    if src is not None:
                        if src[0:4] != 'http':
                            src = crawl_task. url + '/' + src

                        if self.manager.is_valid_image(src):
                            images_src.append(src)
            except StaleElementReferenceException:
                pass

            for src in images_src:
                filename = src[src.rfind('/') + 1:]

                r = requests.get(src, allow_redirects=True, verify=False)
                # open('test-img', 'wb').write(r.content)

                image = Image()
                image.filename = filename
                image.content_type = filename[filename.rfind('.'):]
                image.data = r.content

                crawl_results.images.append(image)


