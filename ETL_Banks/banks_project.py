# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
import lxml


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    response = requests.get(url).text
    content = BeautifulSoup(response, 'lxml')

    df = pd.DataFrame(columns=table_attribs)

    table = content.find('tbody')
    rows = table.find_all('tr')
    for row in rows:
        tds = row.find_all('td')
        if len(tds)!=0 and tds[1].find('a') is not None:
            a = tds[1].find_all('a')[1]
            data_dict = {
                "Name": a.get_text(strip=True),
                "MC_USD_Billion": tds[2].get_text(strip=True)
            }
            df1 = pd.DataFrame([data_dict])  # pass list of dicts
            df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    # Step 1: Read exchange rate CSV into DataFrame
    exch_df = pd.read_csv(csv_path)
    
    # Step 2: Convert to dictionary
    # Assume exchange CSV columns are: Currency, Rate
    exchange_rate = exch_df.set_index('Currency').to_dict()['Rate']

    # Ensure MC_USD_Billion is numeric
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')

    # Step 3: Add new columns by scaling MC_USD_Billion
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    df['MC_EGP_Billion'] = [np.round(x*exchange_rate['EGP'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "./exchange_rate.csv"
table_attribs = ["Name", "MC_USD_Billion"]
output_path = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"

log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

df = transform(df, csv_path)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, output_path)
log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

query_statements = ["SELECT * FROM Largest_banks", "SELECT AVG(MC_GBP_Billion) FROM Largest_banks", "SELECT Name from Largest_banks LIMIT 5"]
for query_statement in query_statements:
    run_query(query_statement, sql_connection)
log_progress("Process Complete")

sql_connection.close()
log_progress("Server Connection closed")
