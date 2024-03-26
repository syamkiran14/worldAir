import requests
import json
import mysql.connector

# Replace these with your MySQL database credentials
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

count = 0
def fetch(url):
    response = requests.get(url, verify=False)
    data = response.text
    return data

for page in range(1,49):
    urli = f"https://api.openaq.org/v3/locations?limit=1000&page={page}"
    s_data = json.loads(fetch(urli))
    print(urli)
    count = len(s_data['results'])
    for count in range(count):
        source_id = s_data['results'][count]['id']
        loc_name = s_data['results'][count]['name']
        locality = s_data['results'][count]['locality']
        laitude = s_data['results'][count]['coordinates']['latitude']
        longitude = s_data['results'][count]['coordinates']['longitude']
        timezone = s_data['results'][count]['timezone']
        ismobile = s_data['results'][count]['isMobile']
        country_code = s_data['results'][count]['country']['code']
        query = "INSERT INTO LOCATION(SOURCE_ID, NAME, LOCALITY, COORDINATES,TIMEZONE, ISMOBILE,COUNTRY_CODE) VALUES(%s,%s,%s,ST_GeomFromText('POINT(%s %s)'),%s,%s,%s)"
        values = (source_id,loc_name,locality,laitude,longitude,timezone,ismobile,country_code)
        cursor.execute(query,values)

cursor.close()
connection.commit()
connection.close()
