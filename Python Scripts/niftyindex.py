import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd

now = datetime.now()+ timedelta(days=1)
two_years_before = now - timedelta(days=90)
endd=str(now).split()[0]
startd=str(two_years_before).split()[0]

holder= yf.Ticker("^NSEI")
bank_nifty_data = holder.history(start=startd, end=endd,interval='1d')
bank_nifty_data.to_csv('niftyindex.csv')
bank_nifty_data = pd.read_csv('niftyindex.csv')
bank_nifty_data['Date'] = pd.to_datetime(bank_nifty_data['Date']).dt.strftime('%Y-%m-%d')
bank_nifty_data['Date'] = pd.to_datetime(bank_nifty_data['Date'])   
bank_nifty_data.set_index('Date', inplace=True)          
bank_nifty_data.round(2)
bank_nifty_data1 = bank_nifty_data[['Open', 'High', 'Low', 'Close']]

bank_nifty_data1.to_csv("nifty1d.csv")