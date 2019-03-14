import threading

from selenium.webdriver.chrome.options import Options

from crawler.core.manager import TaskManager
from selenium import webdriver


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
        url = self.manager.get_next_frontier()

        if url is None:
            # https://stackoverflow.com/questions/38828578/python-threading-interrupt-sleep
            self.manager.handle_waiting_thread()
            self.event.wait()
        else:
            self.web_driver.get(url)
            # TODO obtain hyperlinks and save images
            links = self.web_driver.find_element_by_tag_name('a')
            images = self.web_driver.find_element_by_tag_name('img')

    def execute_task(self):
        while self.run:
            self.crawl_web()

    def stop(self):
        self.run = False
        self.web_driver.close()
