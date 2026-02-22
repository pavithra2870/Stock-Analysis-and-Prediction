import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd

import sys

if len(sys.argv) > 1:
    print (sys.argv[1])
    daysback = int(sys.argv[1])+2
else:
    daysback = 2
    

now = datetime.now()+ timedelta(days=1)
two_years_before = now - timedelta(days=2 * 30)
endd=str(now).split()[0]
startd=str(two_years_before).split()[0]

holder= yf.Ticker("^NSEBANK")
#bank_nifty_data = holder.history(start=startd, end=endd,interval='15m')

#bank_nifty_data.round(2)
#bank_nifty_data1 = bank_nifty_data[['Open', 'High', 'Low', 'Close']]
#bank_nifty_data1.to_csv('retracelogic.csv')


def max_prev_two_rows(x):
    if len(x) == 3:  # Ensure we have exactly three elements in the rolling window
        prev_row1 = x[0]  # Value of the previous row
        prev_row2 = x[1]  # Value of the row before the previous row
        return max(prev_row1, prev_row2)
    else:
        return np.nan  # Return NaN if there are not enough values
        
def min_prev_two_rows(x):
    if len(x) == 3:  # Ensure we have exactly three elements in the rolling window
        prev_row1 = x[0]  # Value of the previous row
        prev_row2 = x[1]  # Value of the row before the previous row
        return min(prev_row1, prev_row2)
    else:
        return np.nan  # Return NaN if there are not enough values


df = pd.read_csv('retracelogic.csv')
"""
high+261range =6
high+161range = 5
high+range = 4
high+38range = 3
high = 2
bdp=1
wdp = 0
low = -1
low-38range = -2
low-range = -3
low-161range = -4
low-261range = -5
"""



df['Date'] = df['Datetime'].str.split().str[0]
#df['JGD'] = df.apply(lambda x: x['High'] - 0.382*(x['High']-x['Low']), axis = 1)
#df['JWD'] = df.apply(lambda x: x['Low'] + 0.382*(x['High']-x['Low']), axis = 1)
df['Range'] = df.apply(lambda x: x['High'] -x['Low'], axis = 1)
#df['BDP'] = df['JGD'].rolling(3).apply(max_prev_two_rows, raw=True)
#df['WDP'] = df['JWD'].rolling(3).apply(min_prev_two_rows, raw=True)
#df['BDP'] = df['Close'].shift(1) - 0.382 * df['Range'].shift(1)
#df['WDP'] = df['Close'].shift(1) + 0.382 * df['Range'].shift(1)

#df['PHigh'] = df['High'].shift(1) 
#df['PLow'] = df['Low'].shift(1) 

df['FC100'] = df['Close'].shift(1) - df['Range'].shift(1)
df['RC100'] = df['Close'].shift(1) + df['Range'].shift(1)

df['FC323'] = df['Close'].shift(1) - 3.23 * df['Range'].shift(1)
df['RC323'] = df['Close'].shift(1) + 3.23 * df['Range'].shift(1)




new_row = pd.DataFrame(
    {
#     'WDP': [df['Close'].iloc[-1] + 0.382 * df['Range'].iloc[-1]],          
#     'BDP': [df['Close'].iloc[-1] - 0.382 * df['Range'].iloc[-1]],           
#     'PLow': [df['Low'].iloc[-1]] ,   
#     'PHigh': [df['High'].iloc[-1]] ,              
     'FC100': [df['Close'].iloc[-1] - df['Range'].iloc[-1]],           
     'RC100': [df['Close'].iloc[-1] + df['Range'].iloc[-1]], 
     'FC323': [df['Close'].iloc[-1] - 3.23 * df['Range'].iloc[-1]],            
     'RC323': [df['Close'].iloc[-1] + 3.23 * df['Range'].iloc[-1]] 
          } )
     
     
df = pd.concat([df, new_row], ignore_index=True)


state = 0
STATE_MORE_THAN_RC100 = 1
STATE_MORE_THAN_RC323 = 2
STATE_LESS_THAN_FC100 = 3
STATE_LESS_THAN_FC323 = 4
EVT_MORE_THAN_RC100 = 1
EVT_MORE_THAN_RC323 = 2
EVT_LESS_THAN_FC100 = 3
EVT_LESS_THAN_FC323 = 4
EVT_LESS_THAN_RC100 = 5
EVT_MORE_THAN_FC323 = 6

buyBiasCt = 0 
sellBiasCt = 0 
latestFC100 = 0
latestFC323 = 0
latestRC100 = 0
latestRC323 = 0
stateOfAlgo = 0
buySignal = False
sellSignal = False
rl_fc110 = 0
rl_rc110 = 0
track_up_retrace = False
track_down_retrace = False

for i in range(2, len(df)):
    previous_row = df.iloc[i - 1]
    current_row = df.iloc[i]
    p_range = previous_row['Range']
    if (p_range > 130 and p_range < 160):
        rl_fc110 =current_row['FC100']
        rl_rc110 =current_row['RC100']
    if current_row['Close'] > rl_rc110 :
        df.loc[i, 'Breach'] = "RC100 breached"
        track_up_retrace = True
        track_down_retrace = False
    if current_row['Close'] < rl_fc110 :
        df.loc[i, 'Breach'] = "FC100 breached"        
        track_down_retrace = True
        track_up_retrace = False

    df.loc[i, 'rl_fc110'] = rl_fc110
    df.loc[i, 'rl_rc110'] = rl_rc110
    df.loc[i, 'track_up_retrace'] = track_up_retrace
    df.loc[i, 'track_down_retrace'] = track_down_retrace
        
print(df.to_string())
print(len(df))
df.to_csv('retracelogic-processed.csv')

exit()

