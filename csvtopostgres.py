
from dotenv import load_dotenv
import psycopg2
import os
import pandas as pd
import glob
from google.cloud import storage

def csvtodf():#convert to df all csvs


    path = os.listdir("./data")  # use your path
    File_names = []
    for item in path:
        if ".csv" in item:
            File_names.append(item)
    print(File_names)

    # create empty list
    # assign dataset names
    data='data'
    dataset=os.getcwd()+'/'+data+'/'
    df={}
    for file in File_names:
        df[file]=pd.read_csv(dataset+file)

    #table_names=[]
    for k in File_names:
        data_frame=df[k]
        table_names='{0}'.format(k.split(".")[0])
        data_frame.columns = [x.lower().replace(" ", "_").replace("-","_") for x in data_frame.columns]

        replacements={
            'object':'varchar',
            'float64':'float',
            'int64':'int',
            'datetime64':'timestamp',
            'timedelta64[ns]':'varchar'
        }
        col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(data_frame.columns, data_frame.dtypes.replace(replacements)))
        load_dotenv()

        conn = psycopg2.connect(host=os.environ["hostname"],
                                dbname=os.environ["database"],
                                user=os.environ["username"], password=os.environ["password"], port=os.environ["port_id"]

                                )

        cursor=conn.cursor()
        print("opened successfully")
        cursor.execute("drop table if exists %s;" % (table_names))
        cursor.execute("create table %s (%s)" % (table_names,col_str))
        print("{0} created successfully".format(table_names))
        # Save df to csv
        data_frame.to_csv(k, header=data_frame.columns, index=False, encoding='utf-8')
        # Open csv file, and save it as an object
        csv_file = open(k)
        print('file opened in memory')

        # SQL copy statement
        SQL = """
                    COPY %s FROM STDIN WITH
                        CSV
                        HEADER
                        DELIMITER AS ','
                  """
        cursor.copy_expert(sql=SQL % table_names,file=csv_file)
        print('{0} copied to db'.format(table_names))
        cursor.execute("grant select on table %s to public" %table_names)
        conn.commit()
        conn.close()





if __name__ == "__main__":
    csvtodf()


