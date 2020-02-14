import json
from pathlib import Path
from requests import Session
from bs4 import BeautifulSoup
from openpyxl import Workbook, load_workbook
import pandas as pd
from pandas.io.json import json_normalize

# uri = 'https://dom.gosuslugi.ru/interactive-reports/api/rest/services/housesConditionReport/deteriorationTerritories'
# Payload:
# Federal districts
# {"managementTypes":[],"operationYearFrom":1700,"operationYearTo":2020,"territories":[],"withFederalDistricts":false,"territoryCategory":"ADMINISTRATIVE"}
# Subjects of the Russian Federation
# {"managementTypes":[],"operationYearFrom":1700,"operationYearTo":2020,"territories":[],"withFederalDistricts":true,"territoryCategory":"ADMINISTRATIVE"}


class Web:
    default_origin = 'https://dom.gosuslugi.ru/'
    default_url = 'https://dom.gosuslugi.ru/interactive-reports/api/rest/services/housesConditionReport' \
                  '/deteriorationTerritories'

    def __init__(self, headers=None, origin=None, url=None,
                 user_agent='Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                            'Chrome/76.0.3809.132 Safari/537.36',
                 debug=False
                 ):
        self.headers = headers
        self.origin = self.default_origin if origin is None else origin
        self.url = self.default_url if url is None else url
        self.user_agent = user_agent
        self.debug = debug
        self.s = Session()
        self.project_dir_path = Path(__file__).parent
        self.project_results_dir = self.project_dir_path / 'out'
        Path(self.project_results_dir).mkdir(exist_ok=True)

    def load_data(self, year_start: int = 1700, year_end: int = 2020, with_districts: bool = False, **kwargs):
        site = self.s.get(self.origin, verify=False)
        cookies = dict(site.cookies)
        if self.headers:
            self.s.headers.update(self.headers)
        self.s.headers.update({'User-Agent': self.user_agent})
        payload = {
            "managementTypes": [],
            "operationYearFrom": year_start,
            "operationYearTo": year_end,
            "territories": [],
            "withFederalDistricts": with_districts,
            "territoryCategory": "ADMINISTRATIVE"
        }
        return self.s.post(self.url, json=payload, verify=False, cookies=cookies, **kwargs).content

    @staticmethod
    def save_json(json, save_path='out.json', mode='w'):
        with open(save_path, mode) as f:
            f.write(json)


def time_spent(func):
    import time
    from functools import wraps

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()  # Can also use time.process_time()
        func_return_val = func(*args, **kwargs)
        end = time.perf_counter()
        print(f'Total time spent:\n*** Function: {func.__module__}.{func.__name__} '
              f'took: {end - start} seconds ***')
        return func_return_val

    return wrapper


@time_spent
def main():
    w = Web()
    mkd_data = w.load_data().decode('utf-8')
    print('MKD Data', mkd_data)
    w.save_json(mkd_data, w.project_results_dir / 'out.json')
    df = pd.read_json(r'out/out.json')
    print('DF\n', df.head())
    data = json.loads(mkd_data)
    res = json_normalize(data['territory'])
    print(res.head())


if __name__ == "__main__":
    main()
