import time
import json
from os.path import join
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))
import logging
from concurrent.futures import ThreadPoolExecutor
import requests
from tqdm import tqdm


class RequestsParallel:
    def __init__(self, config_path):
        self._config = json.load(open(config_path, 'r'))
        os.makedirs(self._config['logfile_path'], exist_ok=True)
        logfile_path = join(self._config['logfile_path'],
                            'Parser_' + time.strftime("%Y%m%d-%H%M%S") + '.log'
                            )
        logging.basicConfig(filename=logfile_path, level=logging.CRITICAL, format="%(asctime)s:%(message)s")
        self._logger = logging.getLogger('Parser')

        self._headers = self._config['headers']
        self._max_workers_requests = self._config['max_workers_requests']

    def _request_page(self, url):
        self._logger.critical(url)
        try:
            page = requests.get(
                url,
                headers=self._headers,
            )
            return (page, url)
        except:
            self._logger.critical('ERROR: {}'.format(url))
            pass

    def extract(self, urls):
        pool = ThreadPoolExecutor(max_workers=self._max_workers_requests)
        pages = []

        for page in tqdm(pool.map(self._request_page, urls)):
            pages.append(page)

        pages = [page for page in pages if page is not None]

        return pages
