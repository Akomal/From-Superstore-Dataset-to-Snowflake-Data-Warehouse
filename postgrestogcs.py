#create bucket
#add json key
#test buket access
#connect to db
#get table names
#read data in df
#create folders from table names
#upload df to gcs

import os
from google.cloud import storage
import psycopg2
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
def postgretogcs():

    #bucket acces
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=''#api key
    storage_client= storage.Client()
    #create new bucket
    bucket_name='superstore10'

    bucket=storage_client.bucket(bucket_name)
    #connect to db

    conn = psycopg2.connect(host=os.environ["hostname"],
                                    dbname=os.environ["database"],
                                    user=os.environ["username"], password=os.environ["password"], port=os.environ["port_id"]

                                    )

    cursor=conn.cursor()
    query="""SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public'"""
    table_name=[]
    cursor.execute(query)

    d={}
    for table in cursor:
        table_name.append(table[0])
    for table in table_name:
        cursor.execute("select * from %s;" % (table))
        data=cursor.fetchall()
        cols=[]
        for cn in cursor.description:
            cols.append(cn[0])
        d[table]=pd.DataFrame(data=data,columns=cols)
        blob = bucket.blob(table+".csv")
        blob.upload_from_string(d[table].to_csv(), 'text/csv')


if __name__ == "__main__":
    postgretogcs()



