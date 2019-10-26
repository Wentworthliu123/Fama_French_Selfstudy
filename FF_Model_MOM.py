#!/usr/bin/env python
# coding: utf-8

# In[ ]:


##########################################
# Fama French Factors-MOM
# Oct 22 2019
# Created by Xinyu LIU
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

###################
# Connect to WRDS #
###################
conn = wrds.Connection(wrds_username='dachxiu')
#make it a constant portal by creating ppass

###################
# Compustat Block #
###################
comp = conn.raw_sql("""
                    select gvkey, datadate, at, pstkl, txditc,pstkrv, pstk, 
                    seq,
                    ceq
                    from comp.funda
                    where indfmt='INDL' 
                    and datafmt='STD'
                    and popsrc='D'
                    and consol='C'
                    and datadate >= '01/01/1979'
                    """)

##################
# Meanings of variables
#https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95613&_ga=2.220875932.586118538.1571246175-1924758175.1568907974
# gvkey 	Char	6	Global Company Key
# datadate 	Num	8	Data Date
# at 	Num	8	Assets - Total
# pstkl 	Num	8	Preferred Stock - Liquidating Value
# txditc 	Num	8	Deferred Taxes and Investment Tax Credit
# pstk 	Num	8	Preferred/Preference Stock (Capital) - Total
#lt 	Num	8	Liabilities - Total
#ceq 	Num	8	Common/Ordinary Equity - Total

#convert datadate to date fmt
#create a new column for 'year'
comp['datadate']=pd.to_datetime(comp['datadate']) 
comp['year']=comp['datadate'].dt.year

# create preferrerd stock
comp['ps']=np.where(comp['pstkrv'].isnull(), comp['pstkl'], comp['pstkrv'])
comp['ps']=np.where(comp['ps'].isnull(),comp['pstk'], comp['ps'])
comp['ps']=np.where(comp['ps'].isnull(),0,comp['ps'])
#manipulate ps data in the sequense of redemption, liquidating and total value, last resolution is 0
comp['txditc']=comp['txditc'].fillna(0)

# create book equity
#Book value of equity equals to Stockholders Equity + Deferred Tax - Preferred Stocks 
comp['be']=comp['seq']+comp['txditc']-comp['ps']

# number of years in Compustat
#Sort DataFrame by column gvkey and datadate
#Mark cumulative number of each gvkey as of that row, starting from 0
comp=comp.sort_values(by=['gvkey','datadate'])
comp['count']=comp.groupby(['gvkey']).cumcount()
comp=comp[['gvkey','datadate','year','be','inv','count']]
##########################
#以上的部分是没啥问题的














###################
# CRSP Mearge #
###################

###################
# CRSP Block      #
###################
# sql similar to crspmerge macro, below are variables for sas crspmerge 
#%let dsevars=ticker ncusip shrcd exchcd dlprc ;
#%let dsfvars = prc openprc vol ret retx shrout;
#MOM uses daily inputs therefore we should change crsp.msf into crsp.dsf, same for crsp.dsenames
crsp_m = conn.raw_sql("""
                      select a.permno, a.permco, a.date, b.shrcd, b.exchcd,
                      a.ret, a.retx, a.shrout, a.prc
                      from crsp.dsf as a
                      left join crsp.dsenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '01/01/2015' and '12/31/2018'
                      and b.exchcd between 1 and 3
                      """) 
#crsp.dsf refers to CRSP Daily Stock
#crsp.dsenames refers to CRSP Daily Stock Event - Name History


# change variable format to int
crsp_d[['permco','permno','shrcd','exchcd']]=crsp_d[['permco','permno','shrcd','exchcd']].astype(int)

crsp_d['date']=pd.to_datetime(crsp_d['date'])
crsp_d['jdate']=crsp_d['date']
#这里我让Jdate暂时不变
#The 1 in MonthEnd just specifies to move one step forward to the next date that's a month end.

# add delisting return, change crsp.m* into crsp.d*
dlret = conn.raw_sql("""
                     select permno, dlret, dlstdt 
                     from crsp.dsedelist
                     """)
#DSEDELIST		CRSP Daily Stock Event - Delisting
#DLRET 	Num	8	Delisting Return,DLRET is the return of the security after it is delisted. 
#It is calculated by comparing a value after delisting against the price on the security's last trading date. 
#The value after delisting can include a delisting price or the amount from a final distribution.
#DLSTDT 	Num	8	Delisting Date,DLSTDT contains the date (in YYMMDD format) of a security's last price on the current exchange.

#process dlret
dlret.permno=dlret.permno.astype(int)
dlret['dlstdt']=pd.to_datetime(dlret['dlstdt'])
dlret['jdate']=dlret['dlstdt']

#merge dlret and crsp_d
crsp = pd.merge(crsp_d, dlret, how='left',on=['permno','jdate'])
#crsp and dlret share the same column names: permno and jdate
######################
#改到这儿稍微思考一下

#process crsp
crsp['dlret']=crsp['dlret'].fillna(0)
crsp['ret']=crsp['ret'].fillna(0)
crsp['retadj']=(1+crsp['ret'])*(1+crsp['dlret'])-1

# calculate market equity
crsp['me']=crsp['prc'].abs()*crsp['shrout'] 
#market equity equals to price of stock times shares of outstanding

#process crsp
crsp=crsp.drop(['dlret','dlstdt','prc','shrout'], axis=1)
crsp=crsp.sort_values(by=['jdate','permco','me'])

### Aggregate Market Cap ###
# sum of me across different permno belonging to same permco a given date
crsp_summe = crsp.groupby(['jdate','permco'])['me'].sum().reset_index()
# largest mktcap within a permco/date
crsp_maxme = crsp.groupby(['jdate','permco'])['me'].max().reset_index()
# join by jdate/maxme to find the permno
crsp1=pd.merge(crsp, crsp_maxme, how='inner', on=['jdate','permco','me'])
# drop me column and replace with the sum me
crsp1=crsp1.drop(['me'], axis=1)
# join with sum of me to get the correct market cap info
crsp2=pd.merge(crsp1, crsp_summe, how='inner', on=['jdate','permco'])
# sort by permno and date and also drop duplicates
crsp2=crsp2.sort_values(by=['permno','jdate']).drop_duplicates()
# important to have a duplicate check


#########################
这里做不太下去因为不知道december和要算的MOM有什么关系
# keep December market cap
crsp2['year']=crsp2['jdate'].dt.year
crsp2['month']=crsp2['jdate'].dt.month
decme=crsp2[crsp2['month']==12]
decme=decme[['permno','date','jdate','me','year']].rename(columns={'me':'dec_me'})

### July to June dates
crsp2['ffdate']=crsp2['jdate']+MonthEnd(-6)
crsp2['ffyear']=crsp2['ffdate'].dt.year
crsp2['ffmonth']=crsp2['ffdate'].dt.month
crsp2['1+retx']=1+crsp2['retx']
crsp2=crsp2.sort_values(by=['permno','date'])

# cumret by stock
crsp2['cumretx']=crsp2.groupby(['permno','ffyear'])['1+retx'].cumprod()
#cumprod returns the product of the year in this case, which is the cumulative return as time goes by

# lag cumret
crsp2['lcumretx']=crsp2.groupby(['permno'])['cumretx'].shift(1)

# lag market cap
crsp2['lme']=crsp2.groupby(['permno'])['me'].shift(1)

# if first permno then use me/(1+retx) to replace the missing value
crsp2['count']=crsp2.groupby(['permno']).cumcount()
crsp2['lme']=np.where(crsp2['count']==0, crsp2['me']/crsp2['1+retx'], crsp2['lme'])

# baseline me
mebase=crsp2[crsp2['ffmonth']==1][['permno','ffyear', 'lme']].rename(columns={'lme':'mebase'})

# merge result back together
crsp3=pd.merge(crsp2, mebase, how='left', on=['permno','ffyear'])
crsp3['wt']=np.where(crsp3['ffmonth']==1, crsp3['lme'], crsp3['mebase']*crsp3['lcumretx'])

decme['year']=decme['year']+1
decme=decme[['permno','year','dec_me']]

# Info as of June
crsp3_jun = crsp3[crsp3['month']==6]

crsp_jun = pd.merge(crsp3_jun, decme, how='inner', on=['permno','year'])
crsp_jun=crsp_jun[['permno','date', 'jdate', 'shrcd','exchcd','retadj','me','wt','cumretx','mebase','lme','dec_me']]
crsp_jun=crsp_jun.sort_values(by=['permno','jdate']).drop_duplicates()
