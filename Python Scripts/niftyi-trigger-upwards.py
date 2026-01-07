import pandas as pd
import sys 

basebdp = 0
basewdp = 0


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

df = pd.read_csv('niftyi-input.csv')

df['Date'] = df['Date'].str.split().str[0]
df['JGD'] = df.apply(lambda x: x['High'] - 0.382*(x['High']-x['Low']), axis = 1)
df['JWD'] = df.apply(lambda x: x['Low'] + 0.382*(x['High']-x['Low']), axis = 1)
df['Range'] = df.apply(lambda x: x['High'] -x['Low'], axis = 1)
df['BDP'] = df['JGD'].rolling(3).apply(max_prev_two_rows, raw=True)
df['WDP'] = df['JWD'].rolling(3).apply(min_prev_two_rows, raw=True)
df['FC110'] = df['Close'].shift(1) - 1.1039 * df['Range'].shift(1)
df['RC110'] = df['Close'].shift(1) + 1.1039 * df['Range'].shift(1)

df['FC323'] = df['Close'].shift(1) - 3.2361 * df['Range'].shift(1)
df['RC323'] = df['Close'].shift(1) + 3.2361 * df['Range'].shift(1)

df['FC523'] = df['Close'].shift(1) - 5.2361 * df['Range'].shift(1)
df['RC523'] = df['Close'].shift(1) + 5.2361 * df['Range'].shift(1)


df['FC785'] = df['Close'].shift(1) - 7.854 * df['Range'].shift(1)
df['RC785'] = df['Close'].shift(1) + 7.854 * df['Range'].shift(1)
new_row = pd.DataFrame(
    {'BDP': [df['JGD'].iloc[-2:].max()], 
     'WDP': [df['JWD'].iloc[-2:].min()],
     'FC110': [df['Close'].iloc[-1] - 1.1039 * df['Range'].iloc[-1]],      
     'RC110': [df['Close'].iloc[-1] + 1.1039 * df['Range'].iloc[-1]], 
     'FC323': [df['Close'].iloc[-1] - 3.2361 * df['Range'].iloc[-1]],           
     'RC323': [df['Close'].iloc[-1] + 3.2361 * df['Range'].iloc[-1]], 
     'FC523': [df['Close'].iloc[-1] - 5.2361 * df['Range'].iloc[-1]],            
     'RC523': [df['Close'].iloc[-1] + 5.2361 * df['Range'].iloc[-1]],      
     'FC785': [df['Close'].iloc[-1] - 7.854 * df['Range'].iloc[-1]],     
     'RC785': [df['Close'].iloc[-1] + 7.854 * df['Range'].iloc[-1]]
     } )
df = pd.concat([df, new_row], ignore_index=True)
state = 0
for i in range(2, len(df)):
    previous_row = df.iloc[i - 1]
    current_row = df.iloc[i]

    if (current_row['Close'] < current_row['WDP']): 
        df.loc[i, 'Closing'] = "Close Lower Than WDP"
        if current_row['Low'] < previous_row['Low'] and state != 1:
            df.loc[i, 'DateColor'] = "Red"
            latest_buy_trigger = current_row['BDP']
            latest_buy_trigger_loc = i
            state = 1
            
    if (current_row['Close'] > current_row['BDP']):        
        df.loc[i, 'Closing'] = "Close Higher Than BDP"
        if current_row['High'] > previous_row['High'] and state != 2:
            df.loc[i, 'DateColor'] = "Green"       
            latest_sell_trigger = current_row['WDP']
            latest_sell_trigger_loc = i
            state = 2
            
    if (current_row['Close'] > current_row['WDP'] and current_row['Close'] < current_row['BDP']):        
        df.loc[i, 'Closing'] = "Close Between BDP & WDP"        
    
        
print(df.to_string())
print(len(df))
df.to_csv('processed_niftyi.csv')

df.reset_index(drop=True, inplace=True)

# New column to store the index of the first matching row in B
matches = []

for i in reversed(range(len(df))):
    
    high_ati_val = df.loc[i, 'High']

    low_ati_val = df.loc[i, 'Low']
    
    a_val = df.loc[i, 'RC785']
    f_val = df.loc[i, 'FC785']

    rc523_val = df.loc[i, 'RC523']
    fc523_val = df.loc[i, 'FC523']    
    rc323_val = df.loc[i, 'RC323']
    fc323_val = df.loc[i, 'RC323']    
    # Look ahead from i+1 to end
    match_row = df.loc[:i-1]

    match_110_index = match_row[match_row['RC110'] > high_ati_val-10]
    match_323_index = match_row[match_row['RC323'] > high_ati_val-10]
    match_523_index = match_row[match_row['RC523'] > high_ati_val-10]    
    match_785_index = match_row[match_row['RC785'] > high_ati_val-10]

    match_110_index.reset_index(drop=True)
    match_323_index.reset_index(drop=True)
    match_523_index.reset_index(drop=True)
    match_785_index.reset_index(drop=True)
    
    top_4_110_h = match_110_index.sort_values(by='RC110', ascending=True).head(1).index
    if top_4_110_h.tolist(): 
#        print(top_4_110_h.tolist()[0])
        v = top_4_110_h.tolist()[0]
        rc110_val = match_110_index.loc[v,'RC110']
        rc110_val_date = match_110_index.loc[v,'Date']
        rc110_val_id = v
    else:
        rc110_val = 0
        rc110_val_id = 0
        rc110_val_date = ''
    
    top_4_323_h = match_323_index.sort_values(by='RC323', ascending=True).head(1).index
    if top_4_323_h.tolist(): 
#        print(top_4_323_h.tolist()[0])
        v = top_4_323_h.tolist()[0]
        rc323_val = match_323_index.loc[v,'RC323']
        rc323_val_date = match_323_index.loc[v,'Date']
        rc323_val_id = v
        if rc323_val > high_ati_val +10:
            rc323_val = 0
            rc323_val_date = ''    
            rc323_val_id = 0
            
   
    else:
        rc323_val = 0
        rc323_val_date = ''    
        rc323_val_id = 0

    
    top_4_523_h = match_523_index.sort_values(by='RC523', ascending=True).head(1).index
    if top_4_523_h.tolist(): 
#        print(top_4_523_h.tolist()[0])
        v = top_4_523_h.tolist()[0]
        rc523_val = match_523_index.loc[v,'RC523']
        rc523_val_date = match_523_index.loc[v,'Date']
        rc523_val_id = v
    else:
        rc523_val = 0
        rc523_val_date = ''        
        rc523_val_id = 0
    
   
    top_4_785_h = match_785_index.sort_values(by='RC785', ascending=True).head(1).index
    if top_4_785_h.tolist(): 
#        print(top_4_785_h.tolist()[0])
        v = top_4_785_h.tolist()[0]
        rc785_val = match_785_index.loc[v,'RC785']
        rc785_val_date = match_785_index.loc[v,'Date']
        rc785_val_id = v
    else:
        rc785_val = 0
        rc785_val_date = ''            
        rc785_val_id = 0

    
    
    match_110_index_low = match_row[match_row['FC110'] < low_ati_val+10]
    match_323_index_low = match_row[match_row['FC323'] < low_ati_val+10]
    match_523_index_low = match_row[match_row['FC523'] < low_ati_val+10]        
    match_785_index_low = match_row[match_row['FC785'] < low_ati_val+10]

    top_4_110_l = match_110_index_low.sort_values(by='FC110', ascending=False).head(1).index       
    if top_4_110_l.tolist(): 
 #       print(top_4_110_l.tolist()[0])
        v = top_4_110_l.tolist()[0]
        fc110_val = match_110_index_low.loc[v,'FC110']
        fc110_val_date = match_110_index_low.loc[v,'Date']
        fc110_val_id = v
    else:
        fc110_val = 0
        fc110_val_date = ''    
        fc110_val_id = 0
   
    top_4_323_l = match_323_index_low.sort_values(by='FC323', ascending=False).head(1).index
    if top_4_323_l.tolist(): 
  #      print(top_4_323_l.tolist()[0])
        v = top_4_323_l.tolist()[0]
        fc323_val = match_323_index_low.loc[v,'FC323']
        fc323_val_date = match_323_index_low.loc[v,'Date']
        fc323_val_id = v
        if(fc323_val < low_ati_val-10):
            fc323_val = 0
            fc323_val_date = ''      
            fc323_val_id = 0
    else:
        fc323_val = 0
        fc323_val_date = ''      
        fc323_val_id = 0

    
    top_4_523_l = match_523_index_low.sort_values(by='FC523', ascending=False).head(1).index
    if top_4_523_l.tolist(): 
   #     print(top_4_523_l.tolist()[0])
        v = top_4_523_l.tolist()[0]
        fc523_val = match_523_index_low.loc[v,'FC523']
        fc523_val_date = match_523_index_low.loc[v,'Date']
        fc523_val_id = v
    else:
        fc523_val = 0
        fc523_val_date = ''      
        fc523_val_id = 0
    
    top_4_785_l = match_785_index_low.sort_values(by='FC785', ascending=False).head(1).index
    if top_4_785_l.tolist(): 
    #    print(top_4_785_l.tolist()[0])
        v = top_4_785_l.tolist()[0]
        fc785_val = match_785_index_low.loc[v,'FC785']
        fc785_val_date = match_785_index_low.loc[v,'Date']
        fc785_val_id = v
    else:
        fc785_val = 0
        fc785_val_date = ''          
        fc785_val_id = 0
    
    
    idate = df.loc[i, 'Date']   
    if (i!=0):
        PrevRange = df.loc[i-1, 'Range']   
    else:
        PrevRange = 0
    Range = df.loc[i, 'Range']   
    #print(i, idate, high_ati_val, low_ati_val, rc110_val, rc110_val_date, rc323_val,rc323_val_date,rc523_val,rc523_val_date,rc785_val,rc785_val_date  )
    
    record =    {"idx" : i,
      "Date": idate,
      "High": high_ati_val,
      "Low":low_ati_val,
      "Range":Range,
      "PrevRange":PrevRange,
      "rc110_val_id":rc110_val_id,
      "rc110_val_date":rc110_val_date,
      "rc110_val":rc110_val,
      "rc323_val_id":rc323_val_id,
      "rc323_val_date":rc323_val_date,
      "rc323_val":rc323_val,
      "rc523_val_id":rc523_val_id,
      "rc523_val_date":rc523_val_date,
      "rc523_val": rc523_val,
      "rc785_val_id":rc785_val_id,
      "rc785_val_date":rc785_val_date,
      "rc785_val":      rc785_val,       
      "fc110_val_id":fc110_val_id,
      "fc110_val_date":fc110_val_date,
      "fc110_val":fc110_val,
      "fc323_val_id":fc323_val_id,
      "fc323_val_date":fc323_val_date,
      "fc323_val":    fc323_val,      
      "fc523_val_id":fc523_val_id,
      "fc523_val_date":fc523_val_date,
      "fc523_val":   fc523_val,  
      "fc785_val_id":fc785_val_id,
      "fc785_val_date":fc785_val_date,
      "fc785_val":         fc785_val    
    }
    matches.append(record)


dfmat = pd.DataFrame(matches)
dfmat.to_csv("matchesniftyi.csv")

"""    
    match_index = match_row[match_row['RC785'] > high_ati_val-10].first_valid_index()
    match_index_low = match_row[match_row['FC785'] < low_ati_val+10].first_valid_index()

    
    match_523_index_low = match_row[match_row['FC523'] < low_ati_val+10].first_valid_index()
"""    
"""            
    if match_index == None and match_index_low == None and match_523_index == match_523_index_low == match_323_index == match_323_index_low == None:
        continue

    if match_index != None :
        b_val = match_row.loc[match_index,'High']
        fdate = match_row.loc[match_index,'Date']
        print("Raise-785", i,idate, a_val, match_index, fdate, b_val, match_index-i, df.loc[i, 'DateColor'] )        
        
    if match_index_low != None :
        bf_val = match_row.loc[match_index_low,'Low']
        bfdate = match_row.loc[match_index_low,'Date']
        print("Fall-785", i,idate, f_val, match_index_low, bfdate, bf_val, match_index_low-i, df.loc[i, 'DateColor'] )
                
    if match_523_index != None :
        highmatch_523_val = match_row.loc[match_523_index,'High']
        highmatch_523_date = match_row.loc[match_523_index,'Date']        
        print("Raise-523", i,idate, rc523_val, match_523_index, highmatch_523_date, highmatch_523_val, match_523_index-i, df.loc[i, 'DateColor'] )                
        
    if match_523_index_low != None :
        lowmatch_523_val = match_row.loc[match_523_index_low,'Low']
        lowmatch_523_date = match_row.loc[match_523_index_low,'Date']                
        print("Fall-523", i,idate, fc523_val, match_523_index_low, lowmatch_523_date, lowmatch_523_val, match_523_index_low-i, df.loc[i, 'DateColor'] )                        
        
    if match_323_index != None :
        highmatch_323_val = match_row.loc[match_323_index,'High']
        highmatch_323_date = match_row.loc[match_323_index,'Date']        
        print("Raise-323", i,idate, rc323_val, match_323_index, highmatch_323_date, highmatch_323_val, match_323_index-i, df.loc[i, 'DateColor'] )                        
        
    if match_323_index_low != None :
        lowmatch_323_val = match_row.loc[match_323_index_low,'Low']
        lowmatch_323_date = match_row.loc[match_323_index_low,'Date']                        
        print("Fall-323", i,idate, fc323_val, match_323_index_low, lowmatch_323_date, lowmatch_323_val, match_323_index_low-i, df.loc[i, 'DateColor'] )                        
"""
    
        
    
#print("Latest Buy trigger", latest_buy_trigger)
#print("Latest Sell trigger", latest_sell_trigger)