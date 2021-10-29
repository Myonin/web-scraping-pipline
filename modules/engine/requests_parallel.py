import time
import json
from os.path import join
import sys
import os
import pickle
import logging
from concurrent.futures import ThreadPoolExecutor
import requests
from tqdm import tqdm
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))


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
        self._mode_save = self._config['mode_save']
        self._out_path = self._config['out_path']
        os.makedirs(self._out_path, exist_ok=True)
        self._pages_saved_path = join(self._out_path, self._config['pages_saved_dir'])

    def _request_page(self, url):
        try:
            page = requests.get(
                url,
                headers=self._headers,
            )
            self._logger.critical(url)
            if page.status_code != requests.status_codes.codes.ok:
                self._logger.critical(page.status_code)
            return (page, url)
        except:
            self._logger.critical('ERROR: {}'.format(url))
            pass

    def extract(self, urls):
        pool = ThreadPoolExecutor(max_workers=self._max_workers_requests)

        if self._mode_save:
            os.makedirs(self._pages_saved_path, exist_ok=True)
            for n, page in tqdm(enumerate(pool.map(self._request_page, urls))):
                with open(join(self._pages_saved_path, str(n)+'.pickle'), 'wb') as handle:
                    pickle.dump(page, handle)

        else:
            pages = []
            for page in tqdm(pool.map(self._request_page, urls)):
                pages.append(page)
            pages = [page for page in pages if page is not None]

            return pages
