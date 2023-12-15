from constants import DATABASE_URL
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def createEngine():
    """Creates the connection to the data base"""
    try:
        engine = create_engine(DATABASE_URL)
        return engine
    
    except Exception as error:
        print(f"Error while creating connection: {error}")

def showPlot(plt, title, xlabel, ylabel):
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    plt.show()


def retrieveData(engine):
    selectQuery = "SELECT * FROM timeseries.observations"
    df = pd.read_sql(selectQuery, engine, index_col=None)
    return df

def averagePrice(df):
    averageClosePrice = df.groupby('symbol')['close'].mean()
    return averageClosePrice

def filterSymbol(df,symbol):
    filteredSymbol = df[df['symbol'] == symbol]
    return filteredSymbol

def numericCorrelation(df):
    """Uses a Pandas dataframe. Shows a heatmap of the numeric correlation between variables"""

    numericDf = df.select_dtypes(include=['float64', 'int64'])
    correlationMatrix = numericDf.corr()
    print(correlationMatrix)
    corrValues = correlationMatrix.values

    fig, ax = plt.subplots(figsize=(10, 8))


    heatmap = ax.imshow(corrValues, cmap='coolwarm', interpolation='nearest')

    for i in range(len(correlationMatrix.columns)):
        for j in range(len(correlationMatrix.columns)):
            text = ax.text(j, i, f'{corrValues[i, j]:.4f}', ha='center', va='center', color='black')


    ax.set_xticks(np.arange(len(correlationMatrix.columns)))
    ax.set_yticks(np.arange(len(correlationMatrix.columns)))
    ax.set_xticklabels(correlationMatrix.columns, rotation=45, ha='right')
    ax.set_yticklabels(correlationMatrix.columns)

    cbar = plt.colorbar(heatmap)

    plt.title("Grid Plot Heatmap with Values")
    plt.show()

def interpolateLinear(engine):
    """Uses a SQLAlchemy Engine. Calculates missing timeseries by using the time_bucket_gapfill function"""
    query = """
                SELECT
                    time_bucket_gapfill('1 day', timestamp) AS bucket_time,
                    *
                FROM timeseries.observations
                ORDER BY bucket_time;
            """

    df = pd.read_sql_query(query, engine)
    engine.dispose()
    df['bucket_time'] = pd.to_datetime(df['bucket_time'])

    plt.figure(figsize=(12, 6))

    symbols = df['symbol'].unique()
    for symbol in symbols:
        symbolData = df[df['symbol'] == symbol]
        plt.plot(symbolData['bucket_time'], symbolData['close'], label=f'{symbol} - Original Data', marker='o', linestyle='-')

        filledGaps = symbolData[symbolData['close'].notna()]
        plt.scatter(filledGaps['bucket_time'], filledGaps['close'], color='red', label=f'{symbol} - Filled Gaps')

    showPlot(plt, "Time Series with Filled Gaps", "Bucket Time", "close")


def calculate_volatility(df):
    """ Uses a Panda Dataframe. Calculate the volatility for each symbol in the DataFrame."""

    df = df.sort_values(by=['timestamp'])

    df['daily_return'] = df.groupby('symbol')['close'].pct_change()
    df['volatility'] = df.groupby('symbol')['daily_return'].rolling(window=20, min_periods=1).std().reset_index(level=0, drop=True)

    plt.figure(figsize=(12, 6))
    for symbol in df['symbol'].unique():
        symbolData = df[df['symbol'] == symbol]
        plt.plot(symbolData['timestamp'], symbolData['volatility'], label=symbol)

    plt.title('Volatility Analysis')
    plt.xlabel('Timestamp')
    plt.ylabel('Volatility')
    plt.legend()
    plt.show()

def averageVolumePerHour(engine):
    """Uses a SQLAlchemy Engine. Calculates the average volume per hour for all the symbols"""

    averageVolumePerHour =   """SELECT
                                    time_bucket('1 hour', timestamp) AS hour_bucket,
                                    symbol,
                                    AVG(volume) AS avg_volume
                                FROM timeseries.observations
                                GROUP BY hour_bucket, symbol
                                ORDER BY hour_bucket;"""
    
    averageVolumePerHourResult = pd.read_sql_query(averageVolumePerHour, engine)
    pivotedDf = averageVolumePerHourResult.pivot(index='hour_bucket', columns='symbol', values='avg_volume')

    plt.figure(figsize=(12, 6))
    pivotedDf.plot(marker='o', linestyle='-', ax=plt.gca())
    showPlot(plt,"Average Volume per Hour for Each Symbol","Hour","Average Volume")

def movingAverageResult(engine):
    """Uses a SQLAlchemy Engine. Calculates the moving average of BTC"""

    print("Moving average of closing prices for a symbol")

    movingAverage = """SELECT
                            timestamp,
                            symbol,
                            close,
                            AVG(close) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg
                        FROM timeseries.observations
                        WHERE symbol = 'BTC_USD'
                        ORDER BY timestamp;"""

    
    movingAverageResult =  pd.read_sql_query(movingAverage, engine)

    plt.figure(figsize=(12, 6))
    plt.plot(movingAverageResult['timestamp'], movingAverageResult['close'], label='Closing Prices', marker='o', linestyle='-')
    plt.plot(movingAverageResult['timestamp'], movingAverageResult['moving_avg'], label='Moving Average', marker='o', linestyle='-', color='orange')
    
    showPlot(plt,"Moving Average of Closing Prices for BTC_USD","Timestamp","Price")
     

def averageTradingVolumeLastQuarter(engine):
    query = """SELECT
                    symbol,
                    AVG(volume) AS average_volume
                FROM
                    timeseries.observations
                WHERE
                    timestamp >= NOW() - INTERVAL '3 months'
                GROUP BY
                    symbol;"""
    
    averageTradingVolumeLastQuarter =  pd.read_sql_query(query, engine)
    print(averageTradingVolumeLastQuarter)
    


def menu(engine):
    """ Display a menu in  the shell to run different analytics tasks
        Needs an SQLAlchemy Engine to run
    """
    code = 1
    print("Retrieving Data")
    df = retrieveData(engine)
    df = df.dropna()

    while code != 0:
        code = int(input("""Select an option:
                        0. Exit
                        1. Average closing price for each symbol
                        2. Filter data based on a symbol
                        3. Correlation of the data
                        4. Average trading volume of the last quarter
                        5. Average volumen per hour
                        6. Select Timeseries with gaps filled
                        7. Moving average of BTC_USD Close
                        8. Calculate volatility for each Symbol
                     """))
        match code:
            case 1:
                print((averagePrice(df)))

            case 2:
                symbol = input("Enter the symbol: ")
                filteredDf = filterSymbol(df,symbol.upper())

                if filteredDf.empty:
                    print("No data found for that symbol")
                else:
                    print(filteredDf.to_string(index=False))

            case 3:
                numericCorrelation(df)

            case 4:
                averageTradingVolumeLastQuarter(engine)
 
            case 5:
                averageVolumePerHour(engine)

            case 6:
                interpolateLinear(engine)
            
            case 7:
                movingAverageResult(engine)

            case 8:
                calculate_volatility(df)

            case _:
                pass
        
    engine.dispose()


def shell():
    print("Starting connection to Database")
    engine = createEngine()
    print("Connection Started Sucessfully")
    print("Starting Menu")
    menu(engine)


shell()