from urllib.robotparser import RobotFileParser

from crawler.database.tables import Site


class SiteMetadata:
    db_site: Site
    rp: RobotFileParser

    def __init__(self, db_site, rp):
        self.db_site = db_site
        self.rp = range

    def can_fetch(self, url):
        if self.rp is not None:
            return self.rp.can_fetch("*", url)
        else:
            return True


class WebPageCrawlTask:
    
    def __init__(self, url, link_from_url, metadata: SiteMetadata = None):
        self.url = url
        self.link_from_url = link_from_url
        self.metadata = metadata
