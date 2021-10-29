import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))
import shutil
import json
import pickle
import time
import logging
from os.path import join
from tqdm import tqdm
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool
from modules.engine.requests_parallel import RequestsParallel
from modules.utilits.utilits import append_csv

NAME_PARSER = 'ZvoniliParser'


class ZvoniliParser(object):
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
        self._out_path = self._config['out_path']
        self._df_phone_numbers = self._config[NAME_PARSER]['df_phone_numbers']
        self._pages_saved_path = join(self._out_path, self._config['pages_saved_dir'])
        self._output_file = join(self._out_path,
                                 self._config[NAME_PARSER]['output_file']+'-'+time.strftime("%Y%m%d-%H%M%S")+'.csv'
                                 )
        self._link_page = self._config[NAME_PARSER]['link_page']
        self._link_page2 = self._config[NAME_PARSER]['link_page2']
        self._max_number_pages = self._config[NAME_PARSER]['max_number_pages']
        self._max_workers_process = self._config['max_workers_process']

    def _generate_phones_urls(self):
        count_pages = self._max_number_pages+1
        urls = [self._link_page.format(number) for number in range(1, count_pages)]
        self._logger.critical('Count of urls: {}'.format(len(urls)))
        return urls

    def _generate_page_urls(self):
        df_phone_numbers = pd.read_csv(join(self._out_path, self._df_phone_numbers+'.csv'))
        urls = [self._link_page2.format(number) for number in df_phone_numbers['phone_numbers'].values]
        self._logger.critical('Count of urls: {}'.format(len(urls)))
        return urls

    def _parse_phones(self):
        list_files = [join(self._pages_saved_path, file) for file in os.listdir(self._pages_saved_path)]
        phone_numbers = []
        for file in tqdm(list_files):
            with open(file, 'rb') as handle:
                page = pickle.load(handle)
                soup = BeautifulSoup(page[0].content)
                soup = soup.find_all('a', {'class': 'nomerlnk'})
                phone_numbers.extend([phone.text for phone in soup])
        self._logger.critical('Count of all phone numbers: {}'.format(len(phone_numbers)))
        phone_numbers = np.unique(phone_numbers)
        self._logger.critical('Count of unique phone numbers: {}'.format(len(phone_numbers)))
        df = pd.DataFrame(phone_numbers, columns=['phone_numbers'])
        df.to_csv(join(self._out_path, self._df_phone_numbers+'.csv'), index=False)

    def _parse_one_page(self, file_path):
        self._logger.critical(file_path)
        with open(file_path, 'rb') as handle:
            page = pickle.load(handle)
        if page[0].status_code == requests.status_codes.codes.ok:
            self._logger.critical(page[1])
            page_content = {}
            soup = BeautifulSoup(page[0].text)
            page_content['number'] = page[1].split('/')[-1]
            page_content['views'] = soup.find_all('span', {'class': 'badge badge-primary'})[0].text
            page_content['tags'] = ', '.join(
                [text.text for text in soup.find_all('span', {'class': 'badge badge-primary'})[2:]])
            page_content['company_region'] = soup.find_all('div', {'class': 'mt-3'})[0].text
            page_content['comments'] = [text.text for text in soup.find_all('blockquote', {'class': 'card-blockquote'})]
            page_content['date'] = [text.text for text in soup.find_all('span', {'style': 'font-size: 14px;'})]
            try:
                append_csv(pd.DataFrame(page_content), self._output_file)
            except:
                self._logger.critical('Diff lens: {}'.format(page[1]))
        else:
            self._logger.critical('STATUS: {} {}'.format(page[0].status_code, page[1]))

    def parse(self):
        self._logger.critical('{}: Start parsing....'.format(NAME_PARSER))
        self._logger.critical('Generating urls for phone number extracting...')
        urls = self._generate_phones_urls()

        self._logger.critical('Start RequestsParallel Phone Numbers...')
        requests_parallel = RequestsParallel(self._config_path)
        requests_parallel.extract(urls)

        self._logger.critical('Parse phone numbers...')
        self._parse_phones()

        self._logger.critical('Generating urls for page extracting...')
        urls = self._generate_page_urls()

        self._logger.critical('Start RequestsParallel Pages...')
        shutil.rmtree(self._pages_saved_path)
        requests_parallel = RequestsParallel(self._config_path)
        requests_parallel.extract(urls)

        self._logger.critical('Start Processing of pages...')
        list_files = [join(self._pages_saved_path, file) for file in os.listdir(self._pages_saved_path)]
        pool = Pool(processes=self._max_workers_process)
        pool.map(self._parse_one_page, list_files)

        self._logger.critical('The end of parsing.')


if __name__ == "__main__":
    parser = ZvoniliParser('../configs.json')
    parser.parse()


