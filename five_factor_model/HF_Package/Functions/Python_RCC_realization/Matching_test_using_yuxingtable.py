#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import numpy as np
import datetime as dt
import wrds
import psycopg2 
import matplotlib.pyplot as plt
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from pandas.core.frame import DataFrame
from scipy import stats
import datetime
import DataHub as hub

startdate = 20000103
enddate = startdate
####
#Read daily stock price from CRSP
####
CRSP = hub.Handle.create('CRSP')
crspdaily=CRSP.read('DailyStock', fields='date,permno,prc,ret',permno='%', start=startdate, end=enddate)
####
#Read stock name history from CRSP, to have the tsymbol information
####
crspname=CRSP.read('NameHistory', fields='permno,ticker,shrcls,namedt,nameendt,tsymbol',permno='%')
crspname=crspname[(crspname.namedt<=startdate) & (crspname.nameendt>=enddate)].sort_values(by=['permno'], ascending=True)
####
#Read TAQ name history from TAQlinktable, to have Yuxing's version of permno-symbol match
####
TAQ = hub.Handle.create('TAQ')
taqlinktable = TAQ.read('DailyLink', date = startdate)
#Read TAQ intraday price, with default permno as 0
####
taq5m=TAQ.read('Daily5Min', date = startdate)
taq5m.drop(columns=['date','permno'], inplace=True)

####
#Count missing prc from CRSP and save the result
####
result=pd.DataFrame(columns = ["crsp_raw_total","crsp_missing_symbol", "crsp_missing_prc","crsp_negative_prc","missymbol_afterclean_misprc"])
crsp_missing_prc=crspdaily[crspdaily.prc.isna()]
crsp_negative_prc=crspdaily[crspdaily.prc<0]
result.loc[0,['crsp_raw_total']]=crspdaily.date.count()
result.loc[0,['crsp_missing_prc']]=crsp_missing_prc.date.count()
result.loc[0,['crsp_negative_prc']]=crsp_negative_prc.date.count()
#Drop nan prc
crspdaily=crspdaily.dropna(axis=0, subset=['prc'])
#Drop negative prc
crspdaily=crspdaily[crspdaily.prc>0]

####
#Count missing tsymbol from CRSP name and save the result
####
crsp_missing_symbol=crspname[crspname.tsymbol.isna()]
result.loc[0,['crsp_missing_symbol']]=len(crspname[crspname.tsymbol.isna()])

####
#Merge CRSP stock price with name info
####
crspwithticker=pd.merge(crspdaily,crspname, on="permno", how="left")
crspwithticker.rename(columns={'tsymbol':'symbol','ticker':'ticker_crsp'}, inplace=True)
crsplinktable=crspwithticker[['permno','symbol','ticker_crsp']]
result.loc[0,['missymbol_afterclean_misprc']]=len(crsplinktable[crsplinktable.symbol.isna()])
result['crsp_cleaned_total']=len(crspdaily)

####
#fill symbol none using table given by yuxing
####
crspwithtiker_yuxing=pd.merge(crspwithticker,taqlinktable, on="permno", how="left")
crspwithtiker_yuxing['symbol']=np.where(crspwithtiker_yuxing['symbol'].isna(),crspwithtiker_yuxing['ticker'],crspwithtiker_yuxing['symbol'])
crspwithtiker_yuxing.drop(columns=['ticker'], inplace=True)
crsp_missing_symbol_after_yuxing=crspwithtiker_yuxing[crspwithtiker_yuxing.symbol.isna()]
result['crsp_missing_symbol_after_yuxing']=len(crsp_missing_symbol_after_yuxing)

####
#Count raw total TAQ from taq5m and save the result
####
result['taq_raw_total']=len(taq5m)

####
#Merge cleaned CRSP stock price with taq price on symbol
####
threshold=0.1
mergeby_tsymbol_symbol=pd.merge(crspwithtiker_yuxing,taq5m,on=['symbol'], how='left')
nomatch=mergeby_tsymbol_symbol[mergeby_tsymbol_symbol.p78.isna()]
result['no_match']=len(nomatch)
ignore_nomatch=mergeby_tsymbol_symbol[~mergeby_tsymbol_symbol.p78.isna()]
ignore_nomatch['invalid']=np.where(((ignore_nomatch.prc/ignore_nomatch.p78-1).abs()<threshold),0,1)
overthreshold = ignore_nomatch[ignore_nomatch['invalid']==1]
#Taking out over threshold stocks
withinthreshold = ignore_nomatch[ignore_nomatch['invalid']==0]
result['successful_match']=len(ignore_nomatch)
result['over_'+str(threshold)]=len(overthreshold)

####
#Generating result
####
finalmatch_crsp_taq=withinthreshold[['permno', 'symbol', 'ticker_crsp', 'prc', 'p78']]
dataout='Matching_test_{}_with_yuxingtable.xlsx'.format(str(startdate))
writer = pd.ExcelWriter(dataout, engine='xlsxwriter')
result.to_excel(writer,sheet_name='descriptive_result')
finalmatch_crsp_taq.to_excel(writer,sheet_name='finalmatch_crsp_taq') 
nomatch.to_excel(writer,sheet_name='nomatch') 
overthreshold.to_excel(writer,sheet_name='overthreshold') 
crsp_missing_symbol_after_yuxing.to_excel(writer,sheet_name='missing_symbol_after_yuxing') 
crsp_missing_symbol.to_excel(writer,sheet_name='crsp_missing_symbol') 
crsp_missing_prc.to_excel(writer,sheet_name='crsp_missing_prc') 
crsp_negative_prc.to_excel(writer,sheet_name='crsp_negative_prc') 
crsplinktable.to_excel(writer,sheet_name='crsplinktable')
taqlinktable.to_excel(writer,sheet_name='taqlinktable_yuxing') 
writer.save()


# In[ ]:


pd.read_csv('Matching_test_20150901_with_yuxingtable.xlsx')

