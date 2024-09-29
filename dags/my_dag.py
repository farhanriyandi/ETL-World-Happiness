from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs4
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.types import String, Float, Integer
from sqlalchemy.exc import SQLAlchemyError
from airflow.hooks.base_hook import BaseHook

default_args = {
    'owner': 'farhan',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define your DAG
dag = DAG(
    'etl_countries_happiness',  # DAG ID
    default_args=default_args,
    description='ETL process for countries and happiness data',
    schedule_interval=None,  # Define a schedule (e.g., '0 12 * * *' for every day at noon)
)


class ExtractTransform:
    def __init__(self):
        pass

    def web_extract_country(self):
        url = 'https://kids.kiddle.co/List_of_countries_by_continents'
        continent_page = requests.get(url).text
        continents_countries_soup = bs4(continent_page, 'lxml')
        continents = continents_countries_soup.find_all('h2' > 'span', {"class": "mw-headline"})
        unwanted_word = ["Antarctica"]
        target_continents = [continent.text for continent in continents if continent.text not in unwanted_word]
        ol_html = continents_countries_soup.find_all('ol')
        all_countries = [countries.find_all('li', {"class": None, "id": None}) for countries in ol_html]
        countries_in_continents = []
        for items in all_countries:
            countries = []
            if items:
                for country in items:
                    countries = [country.find('a').text for country in items if country.find('a')]
                countries_in_continents.append(countries)
        countries_continent_category_df = pd.DataFrame(
            zip(countries_in_continents, target_continents), columns=['Country', 'Continent']
        )
        countries_continent_category_df = countries_continent_category_df.explode(
            "Country").reset_index(drop=True)
        return countries_continent_category_df

    def web_extract_score_happiness(self):
        url = 'https://en.wikipedia.org/wiki/World_Happiness_Report'
        countries_score_page = requests.get(url)
        countries_score_soup = bs4(countries_score_page.content, 'lxml')
        countries_score_table = countries_score_soup.find_all('table', {'class': 'wikitable'})[4]
        countries_score_df = pd.read_html(StringIO(str(countries_score_table)))
        countries_score_df = countries_score_df[0]
        return countries_score_df

    def transform(self, countries_score_df, countries_continent_category_df):
        # Mengubah nama kolom
        countries_continent_category_df = countries_continent_category_df.rename(
            columns={'Continent': 'continent', 'Country': 'country'}
        )
        
        countries_score_df = countries_score_df.rename(columns={
            'Overall rank': 'overall_rank',
            'Country or region': 'country',
            'Score': 'score',
            'GDP per capita': 'gdp_per_capita',
            'Social support': 'social_support',
            'Healthy life expectancy': 'healthy_life_expectancy',
            'Freedom to make life choices': 'freedom_to_make_life_choices',
            'Generosity': 'generosity',
            'Perceptions of corruption': 'perceptions_of_corruption'
        })
        
        merged_df = pd.merge(countries_score_df, countries_continent_category_df, how='inner', on='country')
        return merged_df


class Load:
    def __init__(self):
        self.engine = None
    
    def __create_connection(self):
        conn = BaseHook.get_connection('postgres_default') 
        # user = "airflow"
        # password = "airflow"
        # host = "localhost"
        # database = "postgres"
        # port = 5439
        # conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        conn_string = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        self.engine = create_engine(conn_string)

    def to_postgres(self, db_name: str, data: pd.DataFrame):
        self.__create_connection()
        try:
            df_schema = {
                'overall_rank': Integer,
                'country': String(100),
                'score': Float,
                'gdp_per_capita': Float,
                'social_support': Float,
                'healthy_life_expectancy': Float,
                'freedom_to_make_life_choices': Float,
                'generosity': Float,
                'perceptions_of_corruption': Float,
                'continent': String(100),
            }
            data.to_sql(name=db_name, con=self.engine, if_exists="replace", index=False, schema="public", dtype=df_schema)
        except SQLAlchemyError as err:
            print("error >> ", err.__cause__)

extract_transform = ExtractTransform()
load = Load()

# Define the tasks

def extract_countries(**kwargs):
    ti = kwargs['ti']
    countries_df = extract_transform.web_extract_country()
    ti.xcom_push(key='countries_df', value=countries_df)

def extract_happiness(**kwargs):
    ti = kwargs['ti']
    happiness_df = extract_transform.web_extract_score_happiness()
    ti.xcom_push(key='happiness_df', value=happiness_df)

def transform_data(**kwargs):
    ti = kwargs['ti']
    countries_df = ti.xcom_pull(key='countries_df', task_ids='extract_countries')
    happiness_df = ti.xcom_pull(key='happiness_df', task_ids='extract_happiness')
    result_df = extract_transform.transform(happiness_df, countries_df)
    ti.xcom_push(key='result_df', value=result_df)

def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    result_df = ti.xcom_pull(key='result_df', task_ids='transform_data')
    load.to_postgres(db_name='etl', data=result_df)


# Define Python tasks
task_extract_countries = PythonOperator(
    task_id='extract_countries',
    python_callable=extract_countries,
    provide_context=True,
    dag=dag,
)

task_extract_happiness = PythonOperator(
    task_id='extract_happiness',
    python_callable=extract_happiness,
    provide_context=True,
    dag=dag,
)

task_transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

task_load_to_postgres = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task_extract_countries >> task_extract_happiness >> task_transform_data >> task_load_to_postgres
