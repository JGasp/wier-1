from urllib.robotparser import RobotFileParser


class SiteMetadata:
    db_site_id: int
    rp: RobotFileParser

    def __init__(self, db_site_id, rp):
        self.db_site_id = db_site_id
        self.rp = rp

    def can_fetch(self, url):
        if self.rp is not None:
            return self.rp.can_fetch("*", url)
        else:
            return True


class WebPageCrawlTask:
    
    def __init__(self, url, from_site_id: int = None, metadata: SiteMetadata = None):
        self.url = url
        self.from_site_id = from_site_id
        self.metadata = metadata
