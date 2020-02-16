from pathlib import Path
from requests import Session
from bs4 import BeautifulSoup
from openpyxl import Workbook, load_workbook
import pandas as pd

# Territories URL
# https://dom.gosuslugi.ru/interactive-reports/api/rest/services/housesConditionReport/deteriorationTerritories
# Payload:
# Federal districts
# {"managementTypes":[],"operationYearFrom":1700,"operationYearTo":2020,"territories":[],"withFederalDistricts":false,"territoryCategory":"ADMINISTRATIVE"}
# Subjects of the Russian Federation
# {"managementTypes":[],"operationYearFrom":1700,"operationYearTo":2020,"territories":[],"withFederalDistricts":true,"territoryCategory":"ADMINISTRATIVE"}
# Central district
# {"managementTypes":[],"operationYearFrom":1700,"operationYearTo":2020,"territories":["1"],"withFederalDistricts":true,"territoryCategory":"ADMINISTRATIVE"}
# Moscow district
# {"managementTypes":[],"operationYearFrom":1700,"operationYearTo":2020,"territories":["50"],"withFederalDistricts":true,"territoryCategory":"ADMINISTRATIVE"}
# Houses URL
# https://dom.gosuslugi.ru/interactive-reports/api/rest/services/housesConditionReport/houses
# Moscow district Houses payload
# {"managementTypes":[],"operationYearFrom":1700,"operationYearTo":2020,"territories":["50"],"withFederalDistricts":true,"territoryCategory":"ADMINISTRATIVE","sortBy":"address","sortAsc":true,"pageNumber":1,"pageSize":100}


class Web:
    default_origin = 'https://dom.gosuslugi.ru/'
    territories_url = 'https://dom.gosuslugi.ru/interactive-reports/api/rest/services/housesConditionReport' \
                      '/deteriorationTerritories'
    houses_url = 'https://dom.gosuslugi.ru/interactive-reports/api/rest/services/housesConditionReport/houses'

    def __init__(self, headers=None, origin=None, url=None,
                 user_agent='Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                            'Chrome/76.0.3809.132 Safari/537.36',
                 debug=False
                 ):
        self.headers = headers
        self.origin = self.default_origin if origin is None else origin
        self.url = self.territories_url if url is None else url
        self.user_agent = user_agent
        self.debug = debug
        self.s = Session()
        self.project_dir_path = Path(__file__).parent
        self.project_results_dir = self.project_dir_path / 'out'
        Path(self.project_results_dir).mkdir(exist_ok=True)

    def load_data(self, url=None, **kwargs):
        url = self.url if url is None else url
        site = self.s.get(self.origin, verify=True)
        cookies = dict(site.cookies)
        if self.headers:
            self.s.headers.update(self.headers)
        self.s.headers.update({'User-Agent': self.user_agent})
        return self.s.post(url, verify=True, cookies=cookies, **kwargs)

    @staticmethod
    def save_json(data, save_path='out.json', mode='w'):
        with open(save_path, mode) as f:
            f.write(data)


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
    territories_payload = {
        "managementTypes": [],
        "operationYearFrom": 1799,
        "operationYearTo": 2020,
        "territories": [],
        "withFederalDistricts": False,
        "territoryCategory": "ADMINISTRATIVE"
    }
    houses_payload = {
        "managementTypes": [],
        "operationYearFrom": 1700,
        "operationYearTo": 2020,
        "territories": ["50"],
        "withFederalDistricts": True,
        "territoryCategory": "ADMINISTRATIVE",
        "sortBy": "address",
        "sortAsc": True,
        "pageNumber": 1,
        "pageSize": 100
    }
    w = Web()
    territories_data = w.load_data(json=territories_payload).json()
    print('Terr Data', territories_data)
    w.save_json(str(territories_data), w.project_results_dir / 'out.json')
    df = pd.DataFrame.from_dict(territories_data[0]['children'])
    print('DF\n', df.head())
    print('DF\n', df.info())
    territories = df['territory'].apply(pd.Series)
    territories.columns = ['key'] + ['territory_' + col for col in territories.columns[1:]]
    houses_with_deterioration = df['housesWithDeterioration'].apply(pd.Series)
    houses_with_deterioration.columns = ['housesWithDeterioration_' + col for col in houses_with_deterioration.columns]
    df = pd.concat([territories, houses_with_deterioration, df], axis=1).\
        drop(['territory', 'housesWithDeterioration'], axis=1)
    df.set_index(['key'], inplace=True)
    print('DF\n', df.head())
    print('DF\n', df.info())
    df.to_excel(w.project_results_dir / 'territories.xlsx')

    # houses_data = w.load_data(json=houses_payload).json()

    w.s.close()


if __name__ == "__main__":
    main()
