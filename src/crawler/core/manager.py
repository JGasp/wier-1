from typing import List

from crawler.core.queue import TaskQueue
from crawler.core.task import WebCrawlTask
from crawler.core.job import WebCrawlJob
import multiprocessing
import threading


class TaskManager:

    def __init__(self):
        self.frontier = TaskQueue()
        self.jobs: List[WebCrawlJob] = []
        self.lock = threading.Lock()
        self.thread_sleeping = 0

    def set_frontier(self, urls: []):
        for u in urls:
            self.frontier.add_item(WebCrawlTask(u, None))

    def start(self):
        number_of_threads = multiprocessing.cpu_count()

        for i in range(number_of_threads):
            self.jobs.append(WebCrawlJob(self))

    def get_next_frontier(self):
        with self.lock:
            return self.frontier.get_next()

    def add_new_frontier(self, frontier: WebCrawlTask):
        with self.lock:
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
