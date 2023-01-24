import psycopg2
from io import StringIO
import csv

def executeQuery(query,dataBaseName,datatoinsert=[]):
    conn = psycopg2.connect(
        database=dataBaseName, user='postgres', password='manjith', host='host.docker.internal', port= '5432'
        )
    conn.autocommit=True
    cursor = conn.cursor()
    if(datatoinsert):
        # print(query,datatoinsert)
        cursor.executemany(query,datatoinsert)
        # conn.commit()
        return "data inserted"
    elif("CREATE TABLE" in query):
        # print(query)
        cursor.execute(query)
        # conn.commit()
    else:
        print(query)
        cursor.execute(query)
        data=cursor.fetchall()
        # column_names = [desc[0] for desc in cursor.description]
        # for row in cursor:
        #     a={}
        #     for col in  column_names:
        #         a[col]=row[column_names.index(col)]
        #     data.append(a)
        conn.close()
        return(data)
def insertintodbcsvwriter(dataBaseName,data,colunm_names,tablename):
    conn = psycopg2.connect(
        database=dataBaseName, user='postgres', password='manjith', host='host.docker.internal', port= '5432'
        )
    colm=colunm_names
    conn.autocommit=True
    sio = StringIO()
    writer = csv.writer(sio)
    writer.writerows(data)
    sio.seek(0)
    with conn.cursor() as c:
        c.copy_from(
            file=sio,
            table=tablename,
            columns=colm,
            sep=","
        )
    conn.close()