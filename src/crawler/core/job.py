import threading
from selenium.webdriver.chrome.options import Options

from crawler.core.manager import TaskManager
from selenium import webdriver

from crawler.core.task import WebPageCrawlTask


class WebCrawlJob(threading.Thread):

    def __init__(self, manager: TaskManager):
        super(WebCrawlJob, self).__init__(target=self.execute_task, args=())

        self.run = True
        self.event = threading.Event()
        self.manager = manager

        options = Options()
        options.add_argument("--headless")
        self.web_driver = webdriver.Chrome(options=options)

    def crawl_web(self):
        crawl_task: WebPageCrawlTask = self.manager.get_next_frontier()

        if crawl_task is None:
            # https://stackoverflow.com/questions/38828578/python-threading-interrupt-sleep
            self.manager.handle_waiting_thread()
            self.event.wait()
        else:
            if crawl_task.url.endswith('.pdf') or crawl_task.url.endswith('.doc') or crawl_task.url.endswith('.docx') \
                    or crawl_task.url.endswith('.ppt') or crawl_task.url.endswith('.pptx'):


            else:
                self.web_driver.get(crawl_task)
                hyperlinks = self.web_driver.find_elements_by_tag_name('a')

                for l in hyperlinks:
                    href = l.get_attribute('href')
                    web_page_task = WebPageCrawlTask(href, crawl_task.url)
                    self.manager.add_new_page(web_page_task)

                images = self.web_driver.find_elements_by_tag_name('img')
                for i in images:
                    src = i.get_attribute('src')


    def execute_task(self):
        while self.run:
            self.crawl_web()

    def stop(self):
        self.run = False
        self.web_driver.close()
