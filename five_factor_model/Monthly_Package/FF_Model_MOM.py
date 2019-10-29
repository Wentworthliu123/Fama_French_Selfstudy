#coding=utf-8

#the first line is necessary to run this code on server
##########################################
# Fama French Factors -- Monthly SMB and MOM
# October 24 2019
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
# CRSP Block      #
###################
# sql similar to crspmerge macro
crsp_m = conn.raw_sql("""
                      select a.permno, a.permco, a.date, b.shrcd, b.exchcd,
                      a.ret, a.retx, a.shrout, a.prc
                      from crsp.msf as a
                      left join crsp.msenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '01/01/1978' and '12/31/2018'
                      and b.exchcd between 1 and 3
                      """) 
#crsp.msf refers to Monthly Stock File: Monthly Stock - Securities
#crsp.msenames refers to CRSP Monthly Stock Event - Name History
#PERMNO 	Num	8	PERMNO,PERMNO is a unique five-digit permanent identifier assigned by CRSP to each security in the file
#PERMCO 	Num	8	PERMCO,PERMCO is a unique permanent identifier assigned by CRSP to all companies with issues on a CRSP file
#DATE 	Num	4	Date of Observation,DATE is the date corresponding to CAPV and YEAR
#RET 	Num	8	Returns,A return is the change in the total value of an investment in a common stock over some period of time per dollar of initial investment.
#RETX 	Num	8	Returns without Dividends, Ordinary dividends and certain other regularly taxable dividends are excluded from the returns calculation. The formula is the same as for RET except d(t) is usually 0
#SHROUT 	Num	8	Shares Outstanding,SHROUT is the number of publicly held shares, recorded in thousands
#PRC 	Num	8	Price or Bid/Ask Average,Prc is the closing price or the negative bid/ask average for a trading day.

#SHRCD 	Num	8	Share Code
#EXCHCD 	Num	8	Exchange Code
#NAMEDT 	Num	8	Names Date
#NAMEENDT 	Num	8	Names Ending Date

#The left join treats one table—the left table—as the primary dataset for the join. 
#This means that every row from the left table will be in the result set, 
#even if there’s no rating from the right table. Below, I’ve highlighted the rows that the left join will return.

# change variable format to int
crsp_m[['permco','permno','shrcd','exchcd']]=crsp_m[['permco','permno','shrcd','exchcd']].astype(int)

# Line up date to be end of month
crsp_m['date']=pd.to_datetime(crsp_m['date'])
crsp_m['jdate']=crsp_m['date']+MonthEnd(0)
#The 1 in MonthEnd just specifies to move one step forward to the next date that's a month end.

# add delisting return
dlret = conn.raw_sql("""
                     select permno, dlret, dlstdt 
                     from crsp.msedelist
                     """)
#MSEDELIST		CRSP Monthly Stock Event - Delisting
#DLRET 	Num	8	Delisting Return,DLRET is the return of the security after it is delisted. 
#It is calculated by comparing a value after delisting against the price on the security's last trading date. 
#The value after delisting can include a delisting price or the amount from a final distribution.
#DLSTDT 	Num	8	Delisting Date,DLSTDT contains the date (in YYMMDD format) of a security's last price on the current exchange.

#process dlret
dlret.permno=dlret.permno.astype(int)
dlret['dlstdt']=pd.to_datetime(dlret['dlstdt'])
dlret['jdate']=dlret['dlstdt']+MonthEnd(0)

#merge dlret and crsp_m
crsp = pd.merge(crsp_m, dlret, how='left',on=['permno','jdate'])
#crsp and dlret share the same column names: permno and jdate

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


crsp2['year']=crsp2['jdate'].dt.year
crsp2['month']=crsp2['jdate'].dt.month

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


#######################################################
# Create Momentum-Size Portfolio                           #   
# Measures Based on Past (J) Month Compounded Returns #
#######################################################

J = 11 

ccm5=crsp3[(crsp3['wt']>0) & ((crsp3['shrcd']==10) | (crsp3['shrcd']==11))].copy()
ccm5['ret']=ccm5['retadj']
_tmp_crsp = ccm5[['permno','date','ret','me','exchcd']].sort_values(['permno','date']).set_index('date')
# Replace missing return with 0
_tmp_crsp['ret']=_tmp_crsp['ret'].fillna(0)
# Calculate rolling cumulative return
# by summing log(1+ret) over the formation period
_tmp_crsp['logret']=np.log(1+_tmp_crsp['ret'])
umdr = _tmp_crsp.groupby(['permno','exchcd'])['logret'].rolling(J, min_periods=J).sum()
_tmp_crsp=_tmp_crsp.reset_index()
_tmp_crsp=_tmp_crsp.drop(columns=['logret'])
umdr = umdr.reset_index()
umd = pd.merge(_tmp_crsp,umdr,how='left',on=['date','permno','exchcd'])
umd['raw_cumret']=np.exp(umd['logret'])-1
umd['cumret']=umd.groupby(['permno'])['raw_cumret'].shift(1)
umd=umd.dropna(axis=0, subset=['cumret','me'])
nysemom=umd[umd['exchcd']==1]

nyse_mom=nysemom.groupby(['date'])['cumret'].describe(percentiles=[0.3, 0.7]).reset_index()
nyse_mom=nyse_mom[['date','30%','70%']].rename(columns={'30%':'cumret30', '70%':'cumret70'})

nyse_sz=nysemom.groupby(['date'])['me'].median().to_frame().reset_index().rename(columns={'me':'sizemedn'})
nyse_breaks = pd.merge(nyse_sz, nyse_mom, how='inner', on=['date'])
ccm1_mom = pd.merge(umd, nyse_breaks, how='left', on=['date'])

def sz_bucket(row):
    if 0<row['me']<row['sizemedn']:
        value='S'
    elif row['me']>=row['sizemedn']:
        value='B'
    else:
        value=''
    return value

def mom_bucket(row):
    if row['cumret']<=row['cumret30']:
        value = 'L'
    elif row['cumret']<=row['cumret70']:
        value='M'
    elif row['cumret']>row['cumret70']:
        value='H'
    else:
        value=''
    return value

ccm1_mom['momport']=ccm1_mom.apply(mom_bucket, axis=1)
ccm1_mom['szport']=np.where(ccm1_mom['me']>0, ccm1_mom.apply(sz_bucket, axis=1), '')
ccm1_mom['posbm']=np.where((ccm1_mom['me']>0), 1, 0)
ccm1_mom['nonmissport']=np.where((ccm1_mom['momport']!=''), 1, 0)
ccm1_mom['jdate']=ccm1_mom['date']+MonthEnd(1)
everymom=ccm1_mom[['permno','date', 'jdate', 'momport','szport','posbm','nonmissport']]
ccm6=pd.merge(crsp3, 
        everymom[['permno','jdate','szport','momport','posbm','nonmissport']], how='left', on=['permno','jdate'])
ccm7=ccm6[(ccm6['wt']>0)& (ccm6['posbm']==1) & (ccm6['nonmissport']==1) & 
          ((ccm6['shrcd']==10) | (ccm6['shrcd']==11))]

############################
# Form Fama French Factors #
############################

# function to calculate value weighted return
def wavg(group, avg_name, weight_name):
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return np.nan

# value-weigthed return
vwret=ccm7.groupby(['jdate','szport','momport']).apply(wavg, 'retadj','lme').to_frame().reset_index().rename(columns={0: 'vwret'})
vwret['smport']=vwret['szport']+vwret['momport']

# firm count
vwret_n=ccm7.groupby(['jdate','szport','momport'])['retadj'].count().reset_index().rename(columns={'retadj':'n_firms'})
vwret_n['smport']=vwret_n['szport']+vwret_n['momport']

# tranpose
ff_factors=vwret.pivot(index='jdate', columns='smport', values='vwret').reset_index()
ff_nfirms=vwret_n.pivot(index='jdate', columns='smport', values='n_firms').reset_index()

# create SMB and HML factors
ff_factors['WH']=(ff_factors['BH']+ff_factors['SH'])/2
ff_factors['WL']=(ff_factors['BL']+ff_factors['SL'])/2
ff_factors['WHML'] = ff_factors['WH']-ff_factors['WL']

ff_factors['WB']=(ff_factors['BL']+ff_factors['BM']+ff_factors['BH'])/3
ff_factors['WS']=(ff_factors['SL']+ff_factors['SM']+ff_factors['SH'])/3
ff_factors['WSMB'] = ff_factors['WS']-ff_factors['WB']
ff_factors=ff_factors.rename(columns={'jdate':'date'})

# n firm count
ff_nfirms['H']=ff_nfirms['SH']+ff_nfirms['BH']
ff_nfirms['L']=ff_nfirms['SL']+ff_nfirms['BL']
ff_nfirms['HML']=ff_nfirms['H']+ff_nfirms['L']

ff_nfirms['B']=ff_nfirms['BL']+ff_nfirms['BM']+ff_nfirms['BH']
ff_nfirms['S']=ff_nfirms['SL']+ff_nfirms['SM']+ff_nfirms['SH']
ff_nfirms['SMB']=ff_nfirms['B']+ff_nfirms['S']
ff_nfirms['TOTAL']=ff_nfirms['SMB']
ff_nfirms=ff_nfirms.rename(columns={'jdate':'date'})

# ###################
# # output data #
# ###################
writer = pd.ExcelWriter('FF_Model_MOM8018.xlsx', engine='xlsxwriter')
ff_factors.to_excel(writer,sheet_name='ff_factors') 
ff_nfirms.to_excel(writer,sheet_name='ff_nfirms') 
writer.save()