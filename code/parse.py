import pandas as pd
import logging

def createFrame(data):
    """Uses a pandas data frame with data in format YYYY-MM-DD %H:%M:%S,close value, volume value
    returns a nested dictionary in format: {date:{hour:{close;volume}, hour:{close;volume}}}
    """
    dataDict = {}
    logging.INFO('Starting dataframe transform')
    for index, row in data.iterrows():
        try:
            timestamp = pd.to_datetime(row['timestamp'],format="%Y-%m-%d %H:%M:%S")

            dateKey = timestamp.date()
            timeKey = timestamp.strftime('%H:%M:%S')

            # Create the nested dictionary if it doesn't exist
            dataDict.setdefault(dateKey, {}).setdefault(timeKey, {})

            # Populate the nested dictionary with 'close' and 'volume'
            dataDict[dateKey][timeKey]['close'] = float(row['close'])
            dataDict[dateKey][timeKey]['volume'] = float(row['volume'])
        except Exception as error:
            logging.ERROR(f'Error while trying to parse value: {row}. Cause: {error}')
            pass

    return dataDict


def prepareData(source):
    logging.INFO(f'Reading CSV from {source}')
    df = pd.read_csv(source)

    #We assure that we do not parse any kind of wrong value
    df = df[pd.to_numeric(df['close'], errors='coerce').notnull() & pd.to_numeric(df['volume'], errors='coerce').notnull()]

    forrmattedData = createFrame(df)

    return forrmattedData

def run(source):
    prepareData(source)
