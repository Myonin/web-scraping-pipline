import os
import re
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))
import json
import pickle
import time
import logging
from os.path import join

import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing import Pool
from modules.engine.requests_parallel import RequestsParallel
from modules.utilits.utilits import append_csv

NAME_PARSER = 'KillplsmeParser'

REGEX = {
    'id': '#(\d+)',
     'date': '\d+ [а-я]+ \d\d\d\d, \d\d:\d\d',
    'tags': ' \d+:\d+([а-я, ]+)',
    'likes': '\xa0([-0-9]+)\xa0',
    'text': ' \d+:\d+[а-я, ]+([^END]+)END ',
}

class KillplsmeParser(object):
    def __init__(self, config_path):
        self._config_path = config_path
        self._config = json.load(open(self._config_path, 'r'))
        os.makedirs(self._config['logfile_path'], exist_ok=True)
        logfile_path = join(self._config['logfile_path'],
                            NAME_PARSER+'_'+time.strftime("%Y%m%d-%H%M%S")+'.log'
                            )
        logging.basicConfig(filename=logfile_path, level=logging.CRITICAL, format="%(asctime)s:%(message)s")
        self._logger = logging.getLogger(NAME_PARSER)
        os.makedirs(self._config['out_path'], exist_ok=True)
        self._output_file = join(self._config['out_path'],
                               self._config[NAME_PARSER]['output_file']+time.strftime("%Y%m%d-%H%M%S")+'.csv'
                               )
        self._link_page = self._config[NAME_PARSER]['link_page']
        self._max_number_pages = self._config[NAME_PARSER]['max_number_pages']
        self._max_workers_parse = self._config['max_workers_parse']

    def _generate_urls(self):
        self._logger.critical('Generating urls...')
        count_pages = self._max_number_pages+1
        urls = [self._link_page .format(number) for number in range(1, count_pages)]
        self._logger.critical('Count of urls: {}'.format(len(urls)))
        return urls

    def _parse_one_page(self, page):
        self._logger.critical(page[1])
        text = BeautifulSoup(page[0].text).text
        text = re.sub('\n', '', text)
        text = re.sub(' +', ' ', text)
        text = re.sub('Пристрелить \(\+\)', 'END', text)
        page_content = {}
        for key in REGEX.keys():
            page_content[key] = re.findall(REGEX[key], text)
        try:
            append_csv(pd.DataFrame(page_content), self._output_file)
        except:
            self._logger.critical('ERROR: {}'.format(page[1]))

    def parse(self):
        self._logger.critical('{}: Start parsing....'.format(NAME_PARSER))
        urls = self._generate_urls()

        self._logger.critical('Start RequestsParallel...')
        requests_parallel = RequestsParallel(self._config_path)
        pages = requests_parallel.extract(urls)

        file_pickle = join(
            self._config['out_path'],
            self._config[NAME_PARSER]['output_file']+'.pickle'
             )
        with open(file_pickle, 'wb') as handle:
            pickle.dump(pages, handle)
        self._logger.critical('Pages were saved: {}'.format(file_pickle))

        with open(file_pickle, 'rb') as handle:
            pages = pickle.load(handle)

        self._logger.critical('Start Processing of pages...')
        pool = Pool(processes=self._max_workers_parse)
        pool.map(self._parse_one_page, pages)

        self._logger.critical('The end of parsing.')


if __name__ == "__main__":
    parser = KillplsmeParser('../configs.json')
    parser.parse()


