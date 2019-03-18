import threading

import requests
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options

from selenium import webdriver

from crawler.core.task import WebPageCrawlTask
from crawler.database.tables import Page, Link, Image, PageData


class WebCrawlJob(threading.Thread):

    def __init__(self, manager):
        super(WebCrawlJob, self).__init__(target=self.execute_task, args=())

        self.is_running = True
        self.event = threading.Event()
        self.manager = manager

        options = Options()
        options.add_argument("--headless")
        self.web_driver = webdriver.Chrome('C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe', options=options)
        self.web_driver.implicitly_wait(10)

    def crawl_web(self):
        crawl_task: WebPageCrawlTask = self.manager.get_next_page()

        if crawl_task is None:
            # https://stackoverflow.com/questions/38828578/python-threading-interrupt-sleep
            self.manager.handle_waiting_thread()
            print('[Job %s] Waiting' % threading.get_ident())
            self.event.wait()
            print('[Job %s] Continue' % threading.get_ident())
        else:
            print('[Job %d] Started crawling [%s]' % (threading.get_ident(), crawl_task.url))

            page = Page()
            page.site_id = crawl_task.metadata.db_site_id
            page.url = crawl_task.url

            try:
                r = requests.get(crawl_task.url, verify=False)
                page.http_status_code = r.status_code
            except Exception:
                pass

            if self.manager.is_valid_file(crawl_task.url) and self.manager.download_additional_content(crawl_task.url):
                self.download_binary_file(page, crawl_task)
            else:
                self.process_html_page(page, crawl_task)

    def download_binary_file(self, page, crawl_task):
        page.page_type_code = 'BINARY'
        page_id = self.manager.dataStore.persist(page)

        page_data = PageData()
        page_data.page_id = page_id
        page_data.data_type_code = crawl_task.url[crawl_task.url.rfind('.'):]

        r = requests.get(crawl_task.url, allow_redirects=True, verify=False)
        # open('test', 'wb').write(r.content)

        page_data.data = r.content

        self.manager.dataStore.persist(page_data)
        self.create_link(page_id, crawl_task)


    def process_html_page(self, page, crawl_task):
        try:
            self.web_driver.get(crawl_task.url) # TODO handle redirects
        except TimeoutException:
            return

        page.page_type_code = 'HTML'
        page.html_content = self.web_driver.page_source
        page_id = self.manager.dataStore.persist(page)

        link_href = self.parse_links(crawl_task.url)
        images_src = self.parse_images(crawl_task.url)

        self.add_links_to_frontier(link_href, page_id)
        self.download_images(images_src, page_id)

        self.create_link(page_id, crawl_task)

    def parse_links(self, url):
        hyperlinks = self.web_driver.find_elements_by_tag_name('a')

        # TODO parse JS document.location
        link_href = []
        for l in hyperlinks:
            try:
                href = l.get_attribute('href')
                if href is not None:
                    if href[0:4] != 'http':
                        href = url + '/' + href

                    link_href.append(href)
            except StaleElementReferenceException:
                pass

        return link_href

    def add_links_to_frontier(self, link_href, page_id):
        for href in link_href:
            web_page_task = WebPageCrawlTask(href, page_id)
            self.manager.add_new_page(web_page_task)

    def parse_images(self, url):
        images_src = []
        if self.manager.download_additional_content(url):
            try:
                images = self.web_driver.find_elements_by_tag_name('img')
                for i in images:
                    src = i.get_attribute('src')

                    if src is not None:
                        if src[0:4] != 'http':
                            src = url + '/' + src

                        if self.manager.is_valid_image(src):
                            images_src.append(src)
            except StaleElementReferenceException:
                pass

        return images_src

    def download_images(self, images_src, page_id):
        for src in images_src:
            filename = src[src.rfind('/') + 1:]

            r = requests.get(src, allow_redirects=True, verify=False)
            # open('test-img', 'wb').write(r.content)

            image = Image()
            image.page_id = page_id
            image.filename = filename
            image.content_type = filename[filename.rfind('.'):]
            image.data = r.content

            self.manager.dataStore.persist(image)

    def create_link(self, page_id, crawl_task):
        if crawl_task.from_site_id is not None:
            link = Link()
            link.from_page = crawl_task.from_site_id
            link.to_page = page_id
            self.manager.dataStore.persist(link)

    def execute_task(self):
        while self.is_running:
            self.crawl_web()

    def stop_running(self):
        self.is_running = False
        self.web_driver.close()
