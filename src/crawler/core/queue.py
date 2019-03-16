from collections import deque
from crawler.core.task import WebPageCrawlTask


class TaskQueue:

    def __init__(self):
        self.queue = deque([])

    def add_item(self, item: WebPageCrawlTask):
        self.queue.append(item)

    def get_next(self):
        if self.queue.__len__() == 0:
            return None
        else:
            return self.queue.popleft()

    def item_left(self):
        return self.queue.__len__()
