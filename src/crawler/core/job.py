import threading

import requests
from selenium.webdriver.chrome.options import Options

from selenium import webdriver

from crawler.core.task import WebPageCrawlTask
from crawler.database.tables import Page, Link, Image, PageData


class WebCrawlJob(threading.Thread):

    def __init__(self, manager, identifier):
        super(WebCrawlJob, self).__init__(target=self.execute_task, args=())

        self.identifier = identifier
        self.is_running = True
        self.event = threading.Event()
        self.manager = manager

        options = Options()
        options.add_argument("--headless")
        self.web_driver = webdriver.Chrome('C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe', options=options)

    def crawl_web(self):
        crawl_task: WebPageCrawlTask = self.manager.get_next_page()

        if crawl_task is None:
            # https://stackoverflow.com/questions/38828578/python-threading-interrupt-sleep
            self.manager.handle_waiting_thread()
            self.event.wait()
        else:
            print('Job %d started crawling [%s]' % (self.identifier, crawl_task.url))

            page = Page()
            page.site_id = crawl_task.metadata.db_site_id
            page.url = crawl_task.url

            try:
                r = requests.get(crawl_task.url, verify=False)
                page.http_status_code = r.status_code
            except Exception:
                pass

            if self.manager.is_valid_file(crawl_task.url) and self.manager.download_additional_content(crawl_task.url):
                page.page_type_code = 'BINARY'
                page_id = self.manager.dataStore.persist(page)

                page_data = PageData()
                page_data.page_id = page_id
                page_data.data_type_code = crawl_task.url[crawl_task.url.rfind('.'):]

                r = requests.get(crawl_task.url, allow_redirects=True, verify=False)
                # open('test', 'wb').write(r.content)

                page_data.data = r.content

                self.manager.dataStore.persist(page_data)
            else:
                self.web_driver.get(crawl_task.url)
                # TODO handle redirects

                page.page_type_code = 'HTML'
                page.html_content = self.web_driver.page_source
                page_id = self.manager.dataStore.persist(page)

                hyperlinks = self.web_driver.find_elements_by_tag_name('a')

                # TODO parse JS document.location
                hrefs = []
                for l in hyperlinks:
                    href = l.get_attribute('href')
                    if href is not None:
                        if href[0:4] != 'http':
                            href = crawl_task.url + '/' + href

                        hrefs.append(href)

                srcs = []
                if self.manager.download_additional_content(crawl_task.url):
                    images = self.web_driver.find_elements_by_tag_name('img')
                    for i in images:
                        src = i.get_attribute('src')

                        if src is not None:
                            if src[0:4] != 'http':
                                src = crawl_task.url + '/' + src

                            if self.manager.is_valid_image(src):
                                srcs.append(src)

                for href in hrefs:
                    web_page_task = WebPageCrawlTask(href, page_id)
                    self.manager.add_new_page(web_page_task)

                for src in srcs:
                    filename = src[src.rfind('/')+1:]

                    r = requests.get(src, allow_redirects=True, verify=False)
                    # open('test-img', 'wb').write(r.content)

                    image = Image()
                    image.page_id = page_id
                    image.filename = filename
                    image.content_type = filename[filename.rfind('.'):]
                    image.data = r.content

                    self.manager.dataStore.persist(image)

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
