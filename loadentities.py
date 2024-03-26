#This loads data iteratively into local Database, Mysql

import requests
import json
from urllib.parse import urlparse
import mysql.connector
import datetime
import time
import warnings

warnings.filterwarnings("once")

db_host = ''
db_user = ''
db_password = ''
db_name = ''

connection = mysql.connector.connect(
            host = db_host,
            user = db_user,
            password = db_password,
            database = db_name
        )

def executeQuery(query,values=''):
    cursor = connection.cursor(dictionary=True)   
    try:
        cursor.execute(query,values)
        is_select_query = cursor.description is not None
        if is_select_query:
            result = cursor.fetchall()
            return result
    except mysql.connector.Error as Err:
        error_message = str(Err)

        # Find the start and end positions of the column name within the error message
        start_pos = error_message.find("'") + 1
        end_pos = error_message.find("'", start_pos)

        # Extract the column name
        column_name = error_message[start_pos:end_pos]
        table_pos = query.find("INTO")

        # Find the start and end positions of the next word after "TABLE"
        start_pos = query.find(" ", table_pos) + 1
        end_pos = query.find(" ", start_pos)

        # Extract the table name
        table_name = query[start_pos:end_pos].strip()
        if Err.errno == 1054:
            print('1054')
            alterQuery = f"ALTER TABLE {table_name} ADD COLUMN {column_name} varchar(35);"
            cursor.execute(alterQuery)
            connection.commit()

            executeQuery(query,values)
        elif Err.errno == 1406:
            print(1406)
            fetchLength = "SELECT character_maximum_length FROM information_schema.columns WHERE table_schema = %s AND table_name = %s AND column_name = %s"
            fetchValues = (db_name,table_name,column_name)
            res = executeQuery(fetchLength,fetchValues)
            res = res[0]['CHARACTER_MAXIMUM_LENGTH']
            length = int(res) + 10
            print(length)
            alterQuery = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} varchar({length})"
            executeQuery(alterQuery)
            executeQuery(query,values)
        else:
            print(Err)
            connection.close()
            exit()


    cursor.close()
    connection.commit()


def getEntity(url_string):
    url = urlparse(url_string)
    path_components = url.path.split('/')
    return path_components[2] if len(path_components) > 2 else None

def getData(url):
    for tries in range(10):
        response = requests.get(url,verify=False)
        if response.status_code == 200:
            data = response.text
            return json.loads(data)
        if response.status_code == 408:
            time.sleep(3)
            if tries == 10:
                print(response)
                exit()
      

def parseData(dataDict):
    results = dataDict['results'][0]
    results = flatten_dict(results)
    columns = list(results.keys())
    columnData = list(results.values())
    columnDataType = [getDataType(value) for value in columnData]
    return columns, columnDataType
    
def getDataType(string):
    try:
        print(string)
        print(type(string))
        int_val = int(string)
        return 'bigint'
    except ValueError:
        try:
            float_val = float(string)
            return 'float'
        except ValueError: 
            col_size = len(str(string)) + 10
            return f'Varchar({col_size})'
    except Exception:
        col_size = len(str(string)) + 10
        return f'Varchar({col_size})'

def createEntity(name,column_list,data_types):
    query = f"CREATE TABLE IF NOT EXISTS {name} ({', '.join([f'{col} {dt}' for col, dt in zip(column_list, data_types)])});"
    print(query)
    check = bool(input())
    if check:
        executeQuery(query)
    else:
        exit()

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f'{parent_key}{sep}{k}' if parent_key else k

        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                nested_key = f'{new_key}{sep}{i}'
                if isinstance(item, (dict, list)):
                    items.extend(flatten_dict({i: item}, nested_key, sep=sep).items())
                else:
                    items.append((nested_key, item))
        else:
            items.append((new_key, v))
    return dict(items)

def loadData(name,dataDict):
    dictlength = len(dataDict['results'])
    for i in range(int(dictlength)):
        data =  dataDict['results'][i]
        flatten_data = flatten_dict(data,sep='_')
        colValues = list(flatten_data.values())
        colKeys = list(flatten_data.keys()) 
        placeholders = ", ".join(["%s"] * len(colKeys))
        query = "INSERT INTO {} ({}) VALUES ({})".format(name, ", ".join(colKeys), placeholders)
        executeQuery(query, colValues)   
               

if __name__ == "__main__":

    dataExtracted = {}
    import datetime

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logFile = open(f"{timestamp}.txt", 'w')

    try:
        urlFile = open('urls.txt','r')
        logFile.write("File 'urls.txt' succesfully located\n")
    except FileNotFoundError:
        print(f"File,'urls.txt' is not found")
        exit()

    try:
        for row in urlFile:
            page = 1
            limit = 1000
            row = row.strip()
            
            if '?' in row:
                url = f"{row}&page={page}&limit={limit}"
            else:
                url = f"{row}?page={page}&limit={limit}"
            
            logFile.write(f"traversing url: {url}\n")
            
            table_name = getEntity(row)
            logFile.write(f"Entity extracted as: {table_name}\n")
            
            dataExtracted  = getData(url)
            logFile.write("Data Extracted\n")
           
            found = dataExtracted['meta']['found']
            limit = dataExtracted['meta']['limit']
            logFile.write(f"found:{found}, limit:{limit}\n")
            
            found = found if isinstance(found, int) else int(found.replace('>', '').strip())

            if found:
                Columns, DataTypes = parseData(dataExtracted)
                createEntity(table_name,Columns,DataTypes)
                logFile.write(f"Entity: {table_name} - Created\n")
                logFile.write("Loading Data...")
                loadData(table_name,dataExtracted)  
                logFile.write("Completed\n")
            else:
                print(f"The endpoint: {url} has no data.\n")
                continue

            while (found >= limit):
                page += 1
                if '?' in row:
                    url = f"{row}&page={page}&limit={limit}"
                else:
                    url = f"{row}?page={page}&limit={limit}"
                logFile.write(f"Looping trough page:{page}\n")
                logFile.write(f"URL: {url}\n")
                dataExtracted = getData(url)
                print(dataExtracted['meta'])
                found = dataExtracted['meta']['found']
                limit = dataExtracted['meta']['limit']
                found = found if isinstance(found, int) else int(found.replace('>', '').strip())
                loadData(table_name,dataExtracted)
    except Exception as error: 
        print(error)    
        connection.close()
        urlFile.close()
        logFile.close()
