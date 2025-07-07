from bs4 import BeautifulSoup

from degree72.implementations.actions.http.request_action import RequestAction
from degree72.implementations.blocked_checkers.request_blocked_checker import RequestBlockedChecker
from degree72.implementations.dump_managers.local_dump_manager import LocalDumpManager
from degree72.implementations.daos.csv_dao import CsvDao
from pathlib import Path
from kbhome.Entity import Entity
from kbhome.common_params import *


class ActionRegion(RequestAction):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # run_date_hour_str = self.run_date.strftime('%Y-%m-%d-%H')
        dump_category = 'region'
        csv_name = f'{project_name}_{dump_category}_{self.run_date.date()}.csv'
        if self.debug:
            self.proxy_pool = [self.PROXY_LOCAL_CLASH]
            dump_base_dir = r'F:\\airflow\\Dump'
            csv_path = Path(f'./Data/{csv_name}')
        else:
            # self.proxy_pool = [self.PROXY_SQUID_US_3]
            dump_base_dir = '/home/airflow/Dump'
            csv_path = Path(f'/home/airflow/Data/{project_name}/{csv_name}')
        self.dump_base_dir = dump_base_dir
        self.logger.set_log_level('INFO')
        self.blocked_checker = RequestBlockedChecker(status_codes=[403, 429, 502, 418])
        from degree72.implementations.downloaders.request_downloader import RequestDownloader as MyDownloader
        from degree72.implementations.downloaders.curl_cffi_downloader import CurlCffiDownloader as MyDownloader
        self.downloader = MyDownloader(blocked_checker=self.blocked_checker, proxy_pool=self.proxy_pool)
        self.dumper = LocalDumpManager(run_date=self.run_date, project=project_name, base_dir=dump_base_dir, category=dump_category)
        self.dao = CsvDao(run_date=self.run_date, path=csv_path)
        from degree72.utils.http.header_utils import parse_fiddler_headers
        self.headers = parse_fiddler_headers(headers_raw=headers_html_raw)
    
    def on_run(self, **kwargs):
        self.scrape_data()
        self.dao.write_data()
        return self.dao.path.as_posix()
    
    def scrape_data(self, **kwargs):
        url = 'https://www.kbhome.com/new-homes-palm-coast-area'
        page = self.download_page(url, headers=self.headers)
        soup = BeautifulSoup(page, 'html.parser')
        region_block = soup.select_one('ul.region-dd-list')
        for each in region_block.select('li'):
            if 'class="state"' in str(each): #  过滤掉state级别的url
                continue
            href = each.a.attrs.get('href')
            url = 'https://www.kbhome.com' + href
            region_name = each.text.strip()
            entity = {
                'region_name': region_name,
                'url': url
            }
            self.dao.save(entity)

    
    def download_page(self, url, headers, post_data=None, page_name=None):
        saved_content = self.dumper.load(url, file_name=page_name)
        if saved_content and not self.blocked_checker.is_bad_page(saved_content):
            page = saved_content
        else:
            if post_data:
                response = self.downloader.post(url, headers=headers, data=post_data)
            else:
                response = self.downloader.get(url, headers=headers)
            if not response:
                self.logger.error('failed to download page', url)
                return
            page = response.text
            self.dumper.save(page, url=url, file_name=page_name)
        return page


if __name__ == '__main__':
    from datetime import datetime
    run_date = None
    # run_date = datetime(2025, 6, 5)
    t = ActionRegion(run_date=run_date)
    t.run()
