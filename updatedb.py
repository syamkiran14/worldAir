import requests
import mysql.connector

db_host = 'localhost'
db_user = 'root'
db_password = 'samio.db'
db_name = 'worldair'

connection = mysql.connector.connect(
            host = db_host,
            user = db_user,
            password = db_password,
            database = db_name
        )

/home/samlinux/airflow_env/airflow
airflow users create -u airflow -f airflow -l airflow -r Admin -e airflow@gmail.com
