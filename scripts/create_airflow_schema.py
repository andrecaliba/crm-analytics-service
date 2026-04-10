"""Creates the airflow schema in Postgres if it doesn't exist."""
import psycopg2
import os

url = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].split("?")[0]
conn = psycopg2.connect(url)
conn.autocommit = True
conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS airflow")
conn.close()
print("Airflow schema ready.")