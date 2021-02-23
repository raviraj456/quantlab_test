import logging as log
log.basicConfig(filename='out.log',
        level=log.INFO,
        format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s',)
import pandas as pd
import os.path
import time
import datetime as dt

log.info('Starting run at ' + str( dt.datetime.now()))

def cleandata(df):
    # All fields are essential in describing a complete transaction, therefore
    # check if there is any null values at all in the df
    #       if there is, delete the row
    if df.isnull().values.any():
        log.info("Dropping rows with Null values for any field")
        before = len(df)
        df = df.dropna()
        after = len(df)
        log.info("Dropped " + str(before-after) + " rows from the dataframe for Null values")
    return(df)
    
def readfile(path):
    '''
    checking if the file exists on the path,
        if it exists,
            try reading
    '''
    if os.path.isfile(path):
        log.info("Reading CSV: " + path)
        try:
            dframe = pd.read_csv(path,header=None)
            log.info("Data read. Created a dataframe called dframe")
            log.info("No of Rows: ", dframe.shape[0])
            log.info("No of Columns: ", dframe.shape[1])
        except:
            log.error("File Exists but could not read file in path. Check file format/encoding..")
            exit()
    else:
        log.error("File/Location does not exist. Check path/location.")
        exit()
    return (dframe)
    
def summarize(inputpath):
    dframe = readfile(inputpath)
    
    # Naming the columns
    dframe.columns = ['TimeStamp','Symbol','Quantity','Price']
    # Sort the dataframe by the Timestamp, will help us in creating timegaps between trades.
    # Ensures ordered trades in series
    dframe.sort_values('TimeStamp',inplace=True)
    
    # Clean Data
    dframe = cleandata(dframe)
    
    # Creating a field call TxnValue per trade as: Quantity*Price
    dframe['TxnValue'] = dframe['Quantity']*dframe['Price']
    log.info("Successfully created a column called TxnValue, computed as Quantity*Price")
    
    # Creating groups of data based on the Symbol (base step)
    # Creating a groupby object called group
    group = dframe.groupby('Symbol')
    
    # For each group (per ticker/symbol), finding the time gap between consecutive trades, using 0 for the first trades per symbol
    dframe['Gap'] = group['TimeStamp'].transform(lambda x: x.diff(periods=1))
    #using 0 as gap for the first trade and forcing type int for the column
    dframe['Gap'] = dframe['Gap'].fillna(0).astype(int)
    
    # Finding Max Gap between trades for each symbol trade group
    gap = group['Gap'].max()
    
    # Finding Total Volumn for each Symbol : simple sum of all Quantities in the group
    volume = group['Quantity'].sum()
    
    # Finding the weighted average using the sum of TxnValue for a Symbol's trade group divided by the net volume of the group
    weightedavg = (group['TxnValue'].sum()/volume).astype('int')
    
    # Finding Max Price from each symbol's tradebook
    maxprice = group['Price'].max()
    
    # Creating a final dataframe by concatenating all info for unique symbols (already in order), just joining horizontally
    frame = pd.concat([gap,volume,weightedavg,maxprice], axis=1)   
    
    return frame

def main():
    # timing the operation
    # Stating timestamp as t
    t = time.time()
    # Define input and output files
    inputpath = r'input.csv'
    outputpath = r'output.csv'
    # Summarize data
    summary = summarize(inputpath)
    # Write the summarized data to output csv
    summary.to_csv(outputpath,header=None)
    log.info("Output saved at ", outputpath)
    log.info("RunTime: " + str(time.time()-t),"\n")

if __name__ == "__main__":
    main()





