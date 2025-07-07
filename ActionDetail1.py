import json
import re

from bs4 import BeautifulSoup

from degree72.implementations.actions.http.request_action import RequestAction
from degree72.implementations.blocked_checkers.request_blocked_checker import RequestBlockedChecker
from degree72.implementations.dump_managers.local_dump_manager import LocalDumpManager
from degree72.implementations.daos.csv_dao import CsvDao
from pathlib import Path
from kbhome.Entity import Entity
from kbhome.common_params import *
from degree72.utils.csv_utils import get_data_from_csv


class ActionDetail(RequestAction):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # run_date_hour_str = self.run_date.strftime('%Y-%m-%d-%H')
        dump_category = 'detail'
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
        from degree72.implementations.downloaders.curl_cffi_downloader import CurlCffiDownloader as MyDownloader
        self.downloader = MyDownloader(blocked_checker=self.blocked_checker, proxy_pool=self.proxy_pool)
        self.dumper = LocalDumpManager(run_date=self.run_date, project=project_name, base_dir=dump_base_dir,
                                       category=dump_category)
        self.dao = CsvDao(run_date=self.run_date, path=csv_path)
        from degree72.utils.http.header_utils import parse_fiddler_headers
        self.headers = parse_fiddler_headers(headers_raw=headers_html_raw)

    def on_run(self, **kwargs):
        self.scrape_data()
        self.dao.write_data()
        return self.dao.path.as_posix()

    def scrape_data(self, **kwargs):
        region_file = Path(f'./Data/{project_name}_region_{self.run_date.date()}.csv')
        for each in get_data_from_csv(region_file):
            self.extract_data(todo_block=each)

    def extract_data(self, todo_block):
        url = todo_block['url']
        page = self.download_page(url=url, headers=self.headers)

        if not page:
            self.logger.error(f"Failed to download page: {url}")
            return

        # 提取JSON数据
        json_match = re.search(r'var\s*regionMapData\s*=\s*(\{.*?});', page, re.S)
        if not json_match:
            self.logger.error(f"regionMapData not found in page: {url}")
            return

        try:
            json_str = json_match.group(1)
            json_data = json.loads(json_str)
        except Exception as e:
            self.logger.error(f"JSON parsing error: {e} in page: {url}")
            return

        date_scraped = self.run_date.strftime('%Y-%m-%d')

        # 基本信息
        community_info = {
            'builder': 'KB Home',
            'brand': 'KB Home',
            'community': json_data.get('CommunityName', ''),
            'address': json_data.get('Address', ''),
            'city': json_data.get('City', ''),
            'state': json_data.get('StateAbbreviation', ''),
            'zip': json_data.get('ZIP', ''),
            'plan_type': json_data.get('Style', ''),
            'status': json_data.get('CommunityStatus', ''),
            'link': 'https://www.kbhome.com' + json_data.get('PageUrl', '')
        }

        prices = json_data.get('Prices', [])
        sizes = json_data.get('Sizes', [])

        min_length = min(len(prices), len(sizes))

        if min_length == 0:
            self.logger.warning(f"No floor plans found for {community_info['community']}")
            return

        for i in range(min_length):
            price = prices[i]
            sqft = sizes[i]

            if price <= 0 or sqft <= 0:
                continue

            home_id = f"{json_data.get('CommunityId', '')}-{i + 1}"

            plan_record = {
                'date_scraped': date_scraped,
                'plan': "",
                'floors': json_data.get('StoriesDisplayText', ''),
                'bedrooms': json_data.get('BedroomsDisplayText', ''),
                'full_bathrooms': json_data.get('BathroomsDisplayText', ''),
                'garage': json_data.get('GaragesDisplayText', ''),
                'sqft': sqft,
                'price': price,
                'home_id': home_id
            }

            full_record = {**community_info, **plan_record}
            self.dao.save(full_record)

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
    t = ActionDetail(run_date=run_date)
    t.run()
