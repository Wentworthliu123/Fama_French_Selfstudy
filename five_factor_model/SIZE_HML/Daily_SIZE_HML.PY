#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#coding=utf-8

#the first line is necessary to run this code on server
##########################################
# Fama French Factors
# September 29 2019
# Edited by Xinyu LIU
# Originally from Qingyi (Freda) Song Drechsler
##########################################

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

conn = wrds.Connection(wrds_username='dachxiu')
#make it a constant portal by creating ppass

crsp_d = conn.raw_sql("""
                      select a.permno, a.permco, a.date, b.shrcd, b.exchcd,
                      a.ret, a.retx, a.shrout, a.prc
                      from crsp.dsf as a
                      left join crsp.dsenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '07/01/2019' and '07/31/2019'
                      and b.exchcd between 1 and 3
                      """) 
# change variable format to int
crsp_d[['permco','permno','shrcd','exchcd']]=crsp_d[['permco','permno','shrcd','exchcd']].astype(int)

# Line up date to be end of month
crsp_d['date']=pd.to_datetime(crsp_d['date'])
#####################
crsp_d['jdate']=crsp_d['date']+MonthEnd(0)
#The 1 in MonthEnd just specifies to move one step forward to the next date that's a month end.

# add delisting return
dlret_d = conn.raw_sql("""
                     select permno, dlret, dlstdt 
                     from crsp.dsedelist
                     """)
#MSEDELIST		CRSP Monthly Stock Event - Delisting
#DLRET 	Num	8	Delisting Return,DLRET is the return of the security after it is delisted. 
#It is calculated by comparing a value after delisting against the price on the security's last trading date. 
#The value after delisting can include a delisting price or the amount from a final distribution.
#DLSTDT 	Num	8	Delisting Date,DLSTDT contains the date (in YYMMDD format) of a security's last price on the current exchange.

#process dlret
dlret_d.permno=dlret_d.permno.astype(int)
dlret_d['dlstdt']=pd.to_datetime(dlret_d['dlstdt'])
#######################
dlret_d['date']=dlret_d['dlstdt']+MonthEnd(0)

#merge dlret and crsp_m
crspd = pd.merge(crsp_d, dlret_d, how='left',on=['permno','date'])
#crsp and dlret share the same column names: permno and jdate

#process crsp
crspd['dlret']=crspd['dlret'].fillna(0)
crspd['ret']=crspd['ret'].fillna(0)
crspd['retadj']=(1+crspd['ret'])*(1+crspd['dlret'])-1

# calculate market equity
crspd['me']=crspd['prc'].abs()*crspd['shrout'] 
#market equity equals to price of stock times shares of outstanding

#process crsp
crspd=crspd.drop(['dlret','dlstdt','prc','shrout'], axis=1)
crspd=crspd.sort_values(by=['date','permco','me'])

### Aggregate Market Cap ###
# sum of me across different permno belonging to same permco a given date
crspd_summe = crspd.groupby(['date','permco'])['me'].sum().reset_index()
# largest mktcap within a permco/date
crspd_maxme = crspd.groupby(['date','permco'])['me'].max().reset_index()
# join by jdate/maxme to find the permno
crspd1=pd.merge(crspd, crspd_maxme, how='inner', on=['date','permco','me'])
# drop me column and replace with the sum me
crspd1=crspd1.drop(['me'], axis=1)
# join with sum of me to get the correct market cap info
crspd2=pd.merge(crspd1, crspd_summe, how='inner', on=['date','permco'])
# sort by permno and date and also drop duplicates
crspd2=crspd2.sort_values(by=['permno','date']).drop_duplicates()
# important to have a duplicate check

# keep December market cap

### July to June dates
crspd2=crspd2.sort_values(by=['permno','date'])

# cumret by stock

# lag market cap
crspd2['lme']=crspd2.groupby(['permno'])['me'].shift(1)

# if first permno then use me/(1+retx) to replace the missing value
crspd2['count']=crspd2.groupby(['permno']).cumcount()
crspd2['lme']=np.where(crspd2['count']==0, crspd2['me']/(1+crspd2['retx']), crspd2['lme'])
# baseline me

# merge result back together
crspd2['wt']=crspd2['lme']
