
import airflow.utils.dates
from urllib import request
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'start_date':airflow.utils.dates.days_ago(3)
}

def _fetch_pageviews(pagenames, execution_date, **_):
    result = dict.fromkeys(pagenames, 0)
    with open (f"/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
    print(result)

    with open("/tmp/postgres_query.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write("INSERT INTO pageview_counts VALUES("
            F"'{pagename}', {pageviewcount}, '{execution_date}'"
            ");\n")

with DAG('stock_sense', schedule_interval='@hourly', default_args=default_args, template_searchpath="/tmp") as dag:

    get_data = BashOperator(
        task_id='get_data',
        bash_command=(
            "curl -o /tmp/wikipageviews.gz "
            "https://dumps.wikimedia.org/other/pageviews/"
            "{{ execution_date.year}}/"
            "{{ execution_date.year}}-"
            "{{ '{:02}'.format(execution_date.month) }}/"
            "pageviews-{{ execution_date.year}}"
            "{{ '{:02}'.format(execution_date.month) }}"
            "{{ '{:02}'.format(execution_date.day) }}-"
            "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
        )
    )

    extract_gz = BashOperator(
        task_id="extract_gz",
        bash_command="gunzip --force /tmp/wikipageviews.gz"
    )

    fetch_pageviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=_fetch_pageviews,
        op_kwargs={
            "pagenames":{
                "Google",
                "Amazon",
                "Apple",
                "Microsoft",
                "Facebook"
            }
        }
    )

    write_to_postgres = PostgresOperator(
        task_id="write_to_postgres",
        postgres_conn_id="my_postgres",
        sql="postgres_query.sql"
    )



    get_data >> extract_gz >> fetch_pageviews >> write_to_postgres
