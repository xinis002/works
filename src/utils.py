import psycopg2
import requests


def get_url(employee_id):
    """Поиск по названию"""
    try:
        params = {
            "per_page": 20,
            "employer_id": employee_id,
            "only_with_salary": True,
            "area": 113,
            "only_with_vacancies": True
        }
        r = requests.get("https://api.hh.ru/vacancies/", timeout=1, params=params)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print("HTTP Error")
        print(errh.args[0])
        
    return r.json()['items']


def get_company(employee_ids):
    '''Получение списка компаний'''
    company_list = []
    for employee_id in employee_ids:
        co_n = []
        co_url = []
        employee = get_url(employee_id)
        for company in employee:
            co_n.append(company['employer']['name'])
            co_url.append(company['employer']['url'])
        unique_company_name = set(co_n)
        unique_company_url = set(co_url)
        for company in unique_company_name:
            for url in unique_company_url:
                company_list.append({'companies': {'company_name': company, 'company_url': url}})
    return company_list


def get_vacancies(employee_ids):
    """Получение списка вакансий"""
    vacancies_list = []
    for employer_id in employee_ids:
        emp_vacancies = get_url(employer_id)
        for vacancy in emp_vacancies:
            if vacancy['salary']['from'] is not None and vacancy['salary']['to'] is not None:
                vacancies_list.append({'vacancies': {'vacancy_name': vacancy['name'],
                                                     'city': vacancy['area']['name'],
                                                     'salary_from': vacancy['salary']['from'],
                                                     'salary_to': vacancy['salary']['to'],
                                                     'publish_date': vacancy['published_at'],
                                                     'vacancy_url': vacancy['alternate_url'],
                                                     'company_name': vacancy['employer']['name']}})
    return vacancies_list


def create_database(database_name, params):
    """Создание БД и таблиц для сохранения данных о команиях и их вакансиях"""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f'DROP DATABASE {database_name}')
    cur.execute(f'CREATE DATABASE {database_name}')

    conn.close()


def save_data_to_database(company_list, vacansy_list, database_name, params):
    '''Сохранение полученной информации в таблицах'''
    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute('''
               CREATE TABLE companies(
               company_id SERIAL PRIMARY KEY ,
               company_name VARCHAR(150) NOT NULL,
               url_company TEXT
               )
               ''')

    with conn.cursor() as cur:
        cur.execute('''
           CREATE TABLE vacancies(
           vacancy_id SERIAL PRIMARY KEY,
           company_id INT REFERENCES companies(company_id),
           vacancy_name VARCHAR(150) NOT NULL,
           city_name VARCHAR(100),
           publish_date DATE,
           company_name VARCHAR(150) NOT NULL ,
           salary_from INTEGER,
           salary_to INTEGER,
           url_vacancy TEXT
           )
           ''')

    with conn.cursor() as cur:
        for company in company_list:
            company_data = company['companies']
            cur.execute('''
                INSERT INTO companies(company_name, url_company)
                VALUES (%s, %s)
                RETURNING company_id
                ''',
                        (company_data['company_name'], company_data['company_url']))
            company_id = cur.fetchone()[0]
            for vacancy in vacansy_list:
                vacansy_data = vacancy['vacancies']
                cur.execute('''
                    INSERT INTO vacancies(company_name, company_id, vacancy_name, city_name, publish_date, salary_from,
                     salary_to, url_vacancy)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ''',
                            (vacansy_data['company_name'], company_id, vacansy_data['vacancy_name'],
                             vacansy_data['city'], vacansy_data['publish_date'], vacansy_data['salary_from'],
                             vacansy_data['salary_to'], vacansy_data['vacancy_url'])
                            )

    conn.commit()
    conn.close()