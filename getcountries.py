#This script loads country details like name and code into local Mysql DB.

import requests
import json
import mysql.connector

db_host = ''
db_user = ''
db_password = ''
db_name = ''

# Connect to the MySQL database
connection = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)
cursor = connection.cursor(dictionary=True)

url = 'https://api.openaq.org/v3/countries?limit=1000'
response = requests.get(url, verify= False)
print(response)
data_v3 = response.text
data_v3 = json.loads(response.text)
url = 'https://api.openaq.org/v2/countries?limit=1000'
response = requests.get(url, verify= False)
print(response)
data_v2 = response.text
data_v2 = json.loads(response.text)

countries = data_v3['meta']['found']
locations_count = {}
for i in range(countries):
    country_name = data_v2['results'][i]['name']
    country_code = data_v2['results'][i]['locations']
    locations_count[f"{country_name}"] = country_code

for country in range(countries):
    desired_value = 'Default Value if Key Not Found'
    c_name = data_v3['results'][country]['name']
    source_id = data_v3['results'][country]['id']
    country_code = data_v3['results'][country]['code']
    locations = locations_count[f"{c_name}"]
    query = "INSERT INTO COUNTRY (NAME, source_id, country_code, locations) VALUES (%s, %s, %s, %s)" 
    values = (c_name, source_id, country_code, locations)
    cursor.execute(query,values)

cursor.close()
connection.commit()
connection.close()
