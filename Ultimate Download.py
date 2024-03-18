# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: 'Python 3.8.8 64-bit (''base'': conda)'
#     language: python
#     name: python3
# ---

# +
import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt # Plotting
import matplotlib.dates as mdates # Styling dates
# %matplotlib inline
import datetime as dt # For defining dates
import time
import yfinance as yf
import os
import cufflinks as cf
import plotly.express as px
import plotly.graph_objects as go

# Make Plotly work in your Jupyter Notebook
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
init_notebook_mode(connected=True)
# Use Plotly locally
cf.go_offline()

from plotly.subplots import make_subplots

# Used to get data from a directory
import os
from os import listdir
from os.path import isfile, join

import warnings
warnings.simplefilter("ignore")
# -

# ## Constants

# +
PATH = "Wilshire_Stocks/"

# Start end date defaults
S_DATE = "2017-02-01"
E_DATE = "2022-12-06"
S_DATE_DT = pd.to_datetime(S_DATE)
E_DATE_DT = pd.to_datetime(E_DATE)


# -

# ## Get Column Data from CSV

def get_column_from_csv(file, col_name):
    # Try to get the file and if it doesn't exist issue a warning
    try:
        df = pd.read_csv(file)
    except FileNotFoundError:
        print("File Doesn't Exist")
    else:
        return df[col_name]


# ## Get Stock Tickers

os.getcwd()

tickers = get_column_from_csv("Wilshire-5000-Stocks.csv", "Ticker")
print(len(tickers))


# ## Save Stock Data to CSV

# Function that gets a dataframe by providing a ticker and starting date
def save_to_csv_from_yahoo(folder, ticker):
    stock = yf.Ticker(ticker)
    
    try:
        print("Get Data for : ", ticker)
        # Get historical closing price data
        df = stock.history(period="5y")
    
        # Wait 2 seconds
        time.sleep(0.1)
        
        # Remove the period for saving the file name
        # Save data to a CSV file
        # File to save to 
        the_file = folder + ticker.replace(".", "_") + '.csv'
        print(the_file, " Saved")
        df.to_csv(the_file)
    except Exception as ex:
        print("Couldn't Get Data for :", ticker)


# ## Download All Stocks

# +
import threading
# Approx 20 mins
# TODO: timit and increase to 10 cores
class Data_Downloader:
    def downloader(self,kore):
        k1 = (kore-1)*3481//10
        k2 = kore*3481//10
        for x in range(k1,k2):
            print("Downloading")
            save_to_csv_from_yahoo(PATH, tickers[x])
            print("Complete!")
    
    def __init__(self,kore):
        t = threading.Thread(target=self.downloader, args=(kore,))
        t.start()

Data_Downloader(1)
Data_Downloader(2)
Data_Downloader(3)
Data_Downloader(4)
Data_Downloader(5)
Data_Downloader(6)
Data_Downloader(7)
Data_Downloader(8)
Data_Downloader(9)
Data_Downloader(10)
# -


