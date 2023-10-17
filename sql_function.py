import pyodbc
import pandas as pd
import glob
import os
from datetime import datetime



############# connection setup ################################
server = 'heineken-sql-server-test.database.windows.net'
database = 'heineken_db_test'
username = 'heinekenABC'
password = '{hen@123123}'   
driver= '{ODBC Driver 17 for SQL Server}'

############# table setup ################################
extension = 'csv'
result = glob.glob('*.{}'.format(extension))
filename = result[0][0:-4]

table_spec = open('spec.txt')
table_param = {}
for line in table_spec:
    line = line.strip()
    key_value = line.split('=')
    if len(key_value)==2:
        table_param[key_value[0].strip()] = key_value[1].strip()


############# csv setup ################################
data = pd.DataFrame(pd.read_csv(result[0], sep =','))
data = data.astype(str)
data_tuple = list(data.itertuples(index=False,name=None))   

##copy and remove csv

def __copy2remove__():
    filename_copy = datetime.now().strftime(filename+'-%Y-%m-%d-%H-%M.csv')
    df = pd.read_csv(result[0])
    df.to_csv('csv_log/'+filename_copy, index=False)
    os.remove(result[0])

##check spec with data (if same spec and data column return 1)
def __spec2data__():    
    spec_col = list(table_param.keys())
    data_col = list(data.columns)
    spec_col.sort()
    data_col.sort()
    if spec_col == data_col:
        return 1
    else: return 'spec and data has different column'

##check existing table (if table name already exist in DB return 1)
def __checkexist__():
    sql_col = 'SELECT Column_Name FROM INFORMATION_SCHEMA.COLUMNS where Table_Name = ' + "'" + filename +"'"
    col_ls =[]
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_col)
                for row in cursor:
                    col_ls.append(row[0])
    if len(col_ls) > 0:
        return 1

##check existcolumn (if csv and DB column same return 1)
def __columnsame__():
    sql_col = 'SELECT Column_Name FROM INFORMATION_SCHEMA.COLUMNS where Table_Name = ' + "'" + filename +"'"
    col_ls =[]
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_col)
                for row in cursor:
                    col_ls.append(row[0])
    col_ls.sort()
    data_col = list(data.columns)
    data_col.sort()      
    if col_ls == data_col:
        return 1
    else: return 'csv and DB has different column'

##create table
def __create__():

    s = ''.join(['CREATE TABLE ', filename])
    s1= ''
    for key, value in table_param.items():
        if key != 'table_name':
            s1 += key+' '+value+','
    sql_create= ''.join([s,'(', s1[0:-1], ')']) 

    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_create)
##delete from table
def __delete__():

    delete_stm = "DELETE FROM dbo." + filename
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
            cursor.execute(delete_stm)


##ingest to new table
def __ingest__():

    insert_value = ['?'] * list(data.columns).__len__()

    insert_stmt = (
    "INSERT INTO " + filename + "("+ ','.join(list(data.columns)) +")"
    "VALUES "+ "("+','.join(insert_value)+")"
    )

    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(insert_stmt,data_tuple)

##select from table
def __select__():
    select = "SELECT * FROM dbo." + filename
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
            cursor.execute(select)
            row = cursor.fetchone()
            while row:
                print (str(row[0]) + " " + str(row[1]))
                row = cursor.fetchone()


