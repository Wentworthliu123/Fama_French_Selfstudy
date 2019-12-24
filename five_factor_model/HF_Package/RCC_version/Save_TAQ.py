#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import DataHub as hub
import datetime
import Generating_Daily_Matching
import Portfolio_RCC
import os.path

portfoliopath = '/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/outlier/Portfolio'
outlierpath='/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/outlier/Outlier'
taqoutpath='/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/outlier/TAQdata'
matchoutpath='/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/outlier/TAQmatch'
outpath='/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/outlier/Output'
# Create datatime ticker from df_bar
df_bar=pd.DataFrame()
df_bar[['date','time']] = pd.date_range('09:30', '16:00', freq= '5min').to_series().apply(
            lambda x: pd.Series([i for i in str(x).split(" ")]))
df_bar.index = range(len(df_bar))
    
for year in range(1997, 2017):

    CSV_FILE_PATH = f'{str(year)}0701_0630_daily_all_RCC.csv'
    portfolio_to_read = os.path.join(portfoliopath, CSV_FILE_PATH)
    pdata = pd.read_csv(portfolio_to_read)
    pdata['date']=pd.to_datetime(pdata['date'])
    datecomplete = list(map(lambda x: x.strftime("%Y%m%d"),pdata.date))
    datelist=list(set(datecomplete))
    datelist.sort()

    TAQ = hub.Handle.create('TAQ')
    for eachday in datelist:
        kd = Generating_Daily_Matching.matchingtable(int(eachday))
        kd['date']=pd.to_datetime(eachday)
        # Save daily matchtable to the disc
        matchtable_to_save = os.path.join(matchoutpath, "matchtable_"+str(eachday)+".csv")
        kd.to_csv(matchtable_to_save, index=False)

#    for eachday in datelist:
#        # Extract 5min data from API and save them to p5m dataframe, adding index and date for later merge operation
#        eachday_p5m=TAQ.read('Daily5Min', date = eachday)
#        eachday_p5m['date'] = pd.to_datetime(eachday)
#        eachday_p5m=eachday_p5m.drop(['permno'], axis=1)
#        dff = pd.melt(eachday_p5m, id_vars=list(eachday_p5m.columns)[:3], value_vars=list(eachday_p5m.columns)[3:],
#                 var_name='intratime', value_name='tprice')
#        dff = dff.sort_values(by=['date', 'symbol', 'intratime'])
#        dff['intratime'] =dff['intratime'].str[1:].astype(int)
#        dff.reset_index(drop='true')
#        taq_to_save = os.path.join(taqoutpath, "taq_"+str(eachday)+".csv")
#        dff.to_csv(taq_to_save, index=False)