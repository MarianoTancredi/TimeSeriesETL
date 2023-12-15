import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from constants import DATABASE_URL,RETRIEVE_DATE,PRE_ACTION_QUERY,TABLE_NAME,SCHEMA,NUMERIC_COLUMNS,TIME_COLUMN,FILE_SOURCE,UPDATE_HISTORIC

def createConnections():
    """Creates the connection to the data base"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        engine = create_engine(DATABASE_URL)
        return cur,conn,engine
    
    except Exception as error:
        print(f"Error while creating connection: {error}")

def retrieveLatestDate(cur,conn):
    """Retrieves latest date available in the database"""
    try:
        cur.execute(RETRIEVE_DATE)
        result = cur.fetchone()[0]
        conn.commit()
        return result
    
    except Exception as error:
        print(f"Error while retrieving date: {error}")

def createTable(cur,conn):
    """Requires the cursor and connection. In case it does not exist, creates a hypertable in timescale"""
    try:
        cur.execute(PRE_ACTION_QUERY)
        conn.commit()

    except Exception as error:
        conn.rollback()
        print(f"Error: {error}")

def uploadData(dataframe):
    """Requires a panda dataframe. The function filters the dataframe based on the latest date available in the database and inserts the remaining data"""
    cur,conn,engine = createConnections()
    
    createTable(cur,conn)


    latestDate = retrieveLatestDate(cur,conn)
    if latestDate is None or UPDATE_HISTORIC:
        #Database was empty
        filteredDf = dataframe
    else:
        filteredDf= dataframe[dataframe[TIME_COLUMN].dt.date > (pd.to_datetime(latestDate)).date()]
    

    try:
        filteredDf.to_sql(TABLE_NAME, engine, schema=SCHEMA,if_exists='append', index=False)
        print("Data inserted successfully.")

    except Exception as error:
        print(f"Error: {error}")

    finally:
        # Close cursor,connection and engine
        cur.close()
        conn.close()
        engine.dispose()


def parseData(source):
    """Parses a csv into a dataframe. The columns can be specified in the constants file"""
    df = pd.read_csv(source)

    #Transform numeric columns
    numeric_columns = NUMERIC_COLUMNS
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    #Transform time column to datetime
    df[TIME_COLUMN] = pd.to_datetime(df[TIME_COLUMN])

    #Delete null values
    df = df.dropna()

    return df



def run(source):
    data = parseData(source)
    uploadData(data)

run(FILE_SOURCE)