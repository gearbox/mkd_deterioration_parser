from pathlib import Path
from requests import Session
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
    """
    Class for the network operations
    """
    default_origin = 'https://dom.gosuslugi.ru/'

    def __init__(self, headers=None, origin=None,
                 user_agent='Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                            'Chrome/76.0.3809.132 Safari/537.36'):
        """

        :param headers: Can accept custom headers
        :param origin: Site homepage
        :param user_agent: Web browser user-agent
        """
        self.headers = headers
        self.origin = self.default_origin if origin is None else origin
        self.user_agent = user_agent
        # Создаем сессию
        self.s = Session()
        # Создаем пути и папку для сохранения результатов
        self.project_dir_path = Path(__file__).parent
        self.project_results_dir = self.project_dir_path / 'out'
        Path(self.project_results_dir).mkdir(exist_ok=True)

    def load_data(self, url, **kwargs):
        """
        Downloads data
        :param url: URL to fetch data from
        :param kwargs: Keyword arguments
        :return:
        """
        site = self.s.get(self.origin, verify=True)
        cookies = dict(site.cookies)
        if self.headers:
            self.s.headers.update(self.headers)
        self.s.headers.update({'User-Agent': self.user_agent})
        return self.s.post(url, verify=True, cookies=cookies, **kwargs)

    @staticmethod
    def save_json(data, save_path='out.json', mode='w'):
        """
        Method to save any text object as a text file
        :param data: Data to save as file
        :param save_path: Path and filename
        :param mode: Save mode. 'w' - overwrite file, 'a' - append to file
        :return:
        """
        with open(save_path, mode) as f:
            f.write(data)


class Territories:
    """
    Districts
    """
    def __init__(self, url=None, payload=None):
        territories_payload = {
            "managementTypes": [],
            "operationYearFrom": 1700,
            "operationYearTo": 2020,
            "territories": [],
            "withFederalDistricts": False,
            "territoryCategory": "ADMINISTRATIVE"
        }
        territories_url = 'https://dom.gosuslugi.ru/interactive-reports/api/rest/services/housesConditionReport' \
                          '/deteriorationTerritories'
        self.url = territories_url if url is None else url
        self.payload = territories_payload if payload is None else payload

    @staticmethod
    def dataframe(data: dict):
        """
        Makes a dataframe with Russian districts
        :param data: Dictionary with data (JSON)
        :return:
        """
        # Создаем Dataframe из предварительно скачанного объекта словаря
        dataframe = pd.DataFrame.from_dict(data[0]['children'])
        print('DF\n', dataframe.head())
        print('DF\n', dataframe.info())
        # Берем колонку 'territory', которая содержит словарь и
        # применяем к ней метод pd.Series, который преобразует словарь в колонки.
        # В результате получаем новый Dataframe
        territories = dataframe['territory'].apply(pd.Series)
        print('Territories', territories.head())
        # Переименовываем колонки удобным нам образом
        territories.columns = ['key'] + ['territory_' + col for col in territories.columns[1:]]
        # Берем колонку со словарем 'housesWithDeterioration' и преобразуем в колонки методом pd.Series
        houses_with_deterioration = dataframe['housesWithDeterioration'].apply(pd.Series)
        # Переименовываем колонки удобным нам образом
        houses_with_deterioration.columns = ['housesWithDeterioration_' + col for col in
                                             houses_with_deterioration.columns]
        # Объединяем все датафреймы в один и удаляем более ненужные колонки
        dataframe = pd.concat([territories, houses_with_deterioration, dataframe], axis=1). \
            drop(['territory', 'housesWithDeterioration'], axis=1)
        # Делаем колонку 'key' - индексом
        dataframe.set_index(['key'], inplace=True)
        return dataframe


class HouseAddresses:
    """
    Addresses
    """
    def __init__(self, url=None, payload=None):
        houses_url = 'https://dom.gosuslugi.ru/interactive-reports/api/rest/services/housesConditionReport/houses'
        central_district_payload = {
            "managementTypes": [],
            "operationYearFrom": 1700,
            "operationYearTo": 2020,
            "territories": ["1"],
            "withFederalDistricts": True,
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
            "pageSize": 67700
        }
        self.url = houses_url if url is None else url
        self.payload = houses_payload if payload is None else payload
        self.test_payload = central_district_payload

    @staticmethod
    def dataframe(data: dict):
        dataframe = pd.DataFrame.from_dict(data['items'])
        print('Prepare H-DF\n', dataframe.head())
        print('Prepare H-DF\n', dataframe.info())
        dataframe.set_index(['houseId'], inplace=True)
        return dataframe


def time_spent(func):
    """
    Measures time spent by any function or method. Can be used as a @decorator
    :param func:
    :return:
    """
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
    # Создаем экземпляры классов
    w = Web()
    t = Territories()
    h = HouseAddresses()

    # Тестируем, что нам отдаст сайт в ответ на обычный запрос 'get'
    # test_get = w.s.get('https://dom.gosuslugi.ru/#!/houses-condition/deterioration')
    # print('Test Get: \n', test_get.content.decode('utf-8'))

    # Скачиваем данные в формате JSON
    territories_data = w.load_data(url=t.url, json=t.payload).json()
    # Сохраняем полученные данные в текстовый файл - просто для справки (для работы не требуется)
    w.save_json(str(territories_data), w.project_results_dir / 'territories.json')
    print('Terr Data', territories_data)

    # Создаем датафрейм из полученных данных
    terr_df = t.dataframe(territories_data)
    # Сохраняем датафрейм в файл Excel
    terr_df.to_excel(w.project_results_dir / 'territories.xlsx')
    print('DF\n', terr_df.head())
    print('DF\n', terr_df.info())

    # Получаем данные для Центрального округа (этот шаг необходим для корректной работы следующего шага)
    central_district_data = w.load_data(t.url, json=h.test_payload).json()
    print('Test Data', central_district_data)

    # Получаем данные по домам и адресам
    houses_data = w.load_data(h.url, json=h.payload).json()
    # Сохраняем полученные данные в текстовый файл - просто для справки (для работы не требуется)
    w.save_json(str(houses_data), w.project_results_dir / 'houses.json')
    print('Houses Data', houses_data)

    # Создаем датафрейм из полученных данных
    houses_df = h.dataframe(houses_data)
    # Сохраняем датафрейм в файл Excel
    houses_df.to_excel(w.project_results_dir / 'houses.xlsx')
    print('H-DF\n', houses_df.head())
    print('H-DF\n', houses_df.info())

    # Закрываем сессию (закрываем все соединения с сервером и очищаем память)
    w.s.close()


if __name__ == "__main__":
    main()
