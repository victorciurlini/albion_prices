import yaml
from datetime import datetime, timedelta
import re

def read_yaml(path):
    with open(path) as file:
        cred = yaml.load(file, Loader=yaml.FullLoader)
    
    return cred

def return_datetime():
    dt_today = datetime.now()
    dt_tomorrow = datetime.now() + timedelta(days=1)
    str_date_today = dt_today.strftime("%m-%d-%Y")
    str_date_tomorrow = dt_tomorrow.strftime("%m-%d-%Y")

    return str_date_today, str_date_tomorrow

def get_urls():
    cities, itens = cities_and_itens()
    dt_today, dt_tomorrow = return_datetime()
    dict_anchor = {'today': dt_today, 'tomorrow': dt_tomorrow, 'itens': itens, 'cities': cities}
    cred = read_yaml("config/config.yaml")
    list_urls = [cred['URLS']['URL_GOLD'], cred['URLS']['URL_ITEM']]
    new_urls = []
    for url in list_urls:
        for word, replacement in dict_anchor.items():
            url = re.sub(word, replacement, url)
        new_urls.append(url)
    
    return new_urls

def cities_and_itens():
    list_of_cities = read_file('config/list_of_cities.txt')
    list_of_itens = read_file('config/list_of_itens.txt')
    cities = ','.join(list_of_cities)
    itens = ','.join(list_of_itens)

    return cities, itens

def read_file(path):
    with open(path) as f:
        file_return = [line.strip() for line in f.readlines()]
    
    return file_return