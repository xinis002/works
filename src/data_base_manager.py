import psycopg2

from src.config import config



class DBManager:
    def __init__(self):
        self.db_name = 'hh_company'

    def execute_(self, query):
        conn = psycopg2.connect(dbname=self.db_name, **config())
        with conn:
            with conn.cursor() as cur:
                cur.execute(query)
                results = cur.fetchall()
        conn.close()
        return results

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании."""
        result = self.execute_(f'SELECT companies.company_id, vacancies.company_name,'
                               f' COUNT(company_name) AS "Количество вакансий" '
                               f'FROM companies INNER JOIN vacancies '
                               f'USING (company_name) '
                               f'GROUP BY companies.company_id, vacancies.company_name '
                               f'ORDER BY company_id')
        return result

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на
        вакансию."""
        result = self.execute_(f'SELECT vacancy_name, company_name, salary_from, salary_from, url_vacancy '
                               f'FROM vacancies')
        return result

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        result = self.execute_(f'SELECT AVG(salary_from) AS "Средняя зарплата ОТ", '
                               f'AVG(salary_to) AS "Средняя зарплата ДО" FROM vacancies')
        return result

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        result = self.execute_(f'SELECT * FROM vacancies '
                               f'WHERE salary_from >(SELECT AVG(salary_from) '
                               f'FROM vacancies) AND salary_to > (SELECT AVG(salary_to) FROM vacancies) '
                               f'ORDER BY vacancy_id')
        return result

    def get_vacancies_with_keyword(self):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python."""
        result = self.execute_(f"SELECT * "
                               f"FROM vacancies "
                               f"WHERE vacancy_name LIKE '%инженер%'")
        return result