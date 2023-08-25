import re
from typing import Optional, List, Dict
import os

import psycopg2
import requests
from dotenv import load_dotenv


load_dotenv()

api_key = os.getenv('API_KEY')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')


def check_okved(okveds_for_check: List[str]) -> None:
    """Проверяет список ОКВЭД на валидность"""
    if not okveds_for_check:
        raise ValueError("Необходимо ввести ОКВЭД")
    pattern = re.compile(r'^\d{2}(\.\d{2}(\.\d{1,2})?)?$')
    for okved_for_check in okveds_for_check:
        if not pattern.match(okved_for_check):
            raise ValueError(f"Необходимо правильно ввести список кодов ОКВЭД. Неверный ОКВЭД: {okved_for_check}")


def check_region(region_for_check: int) -> None:
    """Проверяет код региона на валидность"""
    if not region_for_check:
        raise ValueError("Необходимо ввести код региона")
    if not isinstance(region_for_check, int):
        raise ValueError("Необходимо правильно ввести код региона.")


def get_egrul_data(okved_list: List[str], region: int) -> Optional[dict]:
    """
    Получает данные их API https://api.ofdata.ru по заданным параментрам
    """
    check_okved(okved_list)
    check_region(region)

    page = 1
    limit = 100
    results = {}
    try:
        for okved in okved_list:
            results[okved] = []
            while True:
                url = f'https://api.ofdata.ru/v2/search?key={api_key}&by=okved&obj=org&query={okved}&region={region}' \
                      f'&limit={limit}&page={page}'
                response = requests.get(url)

                if response.status_code == 200:
                    page_data = response.json()
                    result_data = page_data.get('data', [])

                    if result_data:
                        results[okved].extend(result_data['Записи'])
                        page += 1
                        total_pages = result_data.get('СтрВсего')

                        if page > total_pages:
                            break
                    else:
                        print("В ответе не найдены необходимые данные")
                else:
                    print(f"API вернул код: {response.status_code}")

        return results

    except requests.exceptions.RequestException as ex:
        raise ValueError(f'Ошибка соединения: {ex}')

    except Exception as ex:
        raise ValueError(f'Ошибка подлключения к API: {ex}')


def insert_data_to_database(companies_data: Dict) -> None:
    """
    Создает таблицу в БД, если она не существует и вносит в нее полученные из API данные.
    """
    try:
        with psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        ) as con:
            with con.cursor() as cur:
                cur.execute('''CREATE TABLE IF NOT EXISTS companies(
                    id SERIAL PRIMARY KEY,
                    company_name VARCHAR(155),
                    okved VARCHAR(155),
                    inn VARCHAR(10),
                    kpp VARCHAR(10),
                    legal_address VARCHAR(155)
                    );''')

                print('Таблица создана успешно.')

                for okved in companies_data:
                    if companies_data.get(okved):
                        for company in companies_data.get(okved):
                            company_name = company.get('НаимПолн')
                            okved_comp = okved
                            inn = company.get('ИНН')
                            kpp = company.get('КПП')
                            legal_address = company.get('ЮрАдрес')

                            cur.execute("INSERT INTO companies (company_name, okved, inn, kpp, legal_address) "
                                        "VALUES (%s, %s, %s, %s, %s)",
                                        (company_name, okved_comp, inn, kpp, legal_address))
                con.commit()
                print("Записи внесены в БД успешно.")
    except psycopg2.Error as ex:
        print(f"DataBase Error: {ex}")


def main():
    """
    Приложение обращается к API сервиса 'ofdata.ru' для получения сведений по компаниям в соотвествии с заданным
    списком ОКВЭД и регионом.
    После успешнрого получения данных, эти сведения (название компании, код ОКВЭД, ИНН, КПП и место регистрации ЮЛ)
    вносятся в базу данных.
    """
    okved = ['62', '62.01', '62.02', '62.02.1', '62.02.2', '62.02.3', '62.02.4', '62.02.9', '62.03', '62.03.11',
             '62.03.12', '62.03.13', '62.03.19', '62.09']
    region = 27

    data = get_egrul_data(okved_list=okved, region=region)
    insert_data_to_database(data)


if __name__ == '__main__':
    main()
