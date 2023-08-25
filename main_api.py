from typing import Optional, List
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


def get_egrul_data(okved_list: List[str], region: int) -> Optional[dict]:
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

        return results

    except Exception as ex:
        raise ValueError(f'Проблема с подключением к API: {ex}')


def database(companies_data):
    try:
        con = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cur = con.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS companies(
            id SERIAL PRIMARY KEY,
            company_name VARCHAR(155),
            okved VARCHAR(155),
            inn VARCHAR(10),
            kpp VARCHAR(10),
            legal_address VARCHAR(155)
            );''')

        print('TABLE CREATE SUCCESFULLY')

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
        print("Record inserted successfully")
    except psycopg2.Error as ex:
        print(f"DataBase Error: {ex}")
    finally:
        if con:
            con.close()


def main():
    okved = ['62', '62.01', '62.02', '62.02.1', '62.02.2', '62.02.3', '62.02.4', '62.02.9', '62.03', '62.03.11',
             '62.03.12', '62.03.13', '62.03.19', '62.09']
    data = get_egrul_data(okved_list=okved, region=27)

    # with open('file_3.json', 'r') as file:
    #     data = json.load(file)
    database(data)


if __name__ == '__main__':
    main()
