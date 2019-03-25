import threading
from collections import deque
from typing import List

from crawler.core.task import WebPageCrawlTask


class TaskQueue:

    lock = threading.Lock()

    current_task_id = 0
    next_task_id = 1

    def __init__(self):
        self.queue = deque([])
        self.out_of_order_tasks = {}

    def add_items(self, items: List[WebPageCrawlTask], from_task_id):
        with self.lock:
            if from_task_id == self.current_task_id:
                self.add_items_to_queue(items)

                self.current_task_id += 1
                while self.current_task_id in self.out_of_order_tasks:
                    items = self.out_of_order_tasks[self.current_task_id]
                    self.add_items_to_queue(items)
                    self.current_task_id += 1
            else:
                self.out_of_order_tasks[from_task_id] = items

    def add_items_to_queue(self, items: List[WebPageCrawlTask]):
        for ct in items:
            ct.id_number = self.next_task_id
            self.next_task_id += 1
            self.queue.append(ct)

    def get_next(self):
        with self.lock:
            if self.queue.__len__() == 0:
                return None
            else:
                return self.queue.popleft()

    def item_left(self):
        return self.queue.__len__()
