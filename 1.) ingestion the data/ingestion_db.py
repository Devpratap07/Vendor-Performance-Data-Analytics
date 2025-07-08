#First python script

import pandas as pd
import os 
from sqlalchemy import create_engine
import logging
import time # kitne time mein complete hoga uske liye 

logging.basicConfig(  #basicCof define the log structure
    filename = "logs/ingestion_db.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",  # time -> level -> msg
    filemode ="a" # a -> append
)


engine = create_engine('sqlite:///inventory.db') # creating engine-- database name -> inventory.db

# in companies we don't have one notbook we hve to script the data
# like continues data in coming so we have to write the script
def ingest_db(df , table_name, engine): #df -> dataframe 
    
    ''' this function will ingest the dataframes into database table''' 
    
    df.to_sql(table_name, con = engine, if_exists = 'replace' , index = False)

def load_raw_data():
    
    '''this function will load the CSVs as dataframes and injest into db'''
    
    start = time.time()
    for file in os.listdir('data'):  #reading file from data
        if'.csv' in file:
            df = pd.read_csv('data/'+file)
            logging.info(f'ingesting {file} in db')
            ingest_db(df,file[:-4], engine) # file read karega fir ingest krega databasse
    end = time.time()
    total_time = (end - start)/60
    logging.info('-------------ingestion complete------------')
    logging.info(f'\ntotal time taken : {total_time} minutes')

if __name__ == '__main__':
    load_raw_data()
