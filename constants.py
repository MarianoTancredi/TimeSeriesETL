HOST = ""
DATABASE = ""
USER = ""
PASSWORD = ""
PORT = ""
DATABASE_URL = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}' 

#<-------------------------------------------------------QUERIES---------------------------------------------------->
RETRIEVE_DATE  = "SELECT MAX(timestamp) FROM timeseries.observations"

PRE_ACTION_QUERY = '''
CREATE TABLE IF NOT EXISTS timeseries (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(255),
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
);

SELECT create_hypertable('timeseries', by_range('timestamp'));'''

#<----------------------------------------------------TABLE/FILE DATA----------------------------------------------------------->

TABLE_NAME = 'observations'
SCHEMA = 'timeseries'
TIME_COLUMN = 'timestamp'
NUMERIC_COLUMNS =  ['low', 'close', 'volume']
FILE_SOURCE = 'trades.csv'
UPDATE_HISTORIC = False