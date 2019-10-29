#coding=utf-8

#the first line is necessary to run this code on server
##########################################
# Fama French Factors -- Monthly Rm_Rf
# October 29 2019
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
from matplotlib.backends.backend_pdf import PdfPages


###################
# Connect to WRDS #
###################
conn = wrds.Connection(wrds_username='dachxiu')
#make it a constant portal by creating ppass

#######################
# CRSP Risk Free Rate #
#######################
#This method extract data directly from crsp which does not necessarily align with the fama version
#I will apply the data provided by fama instead to generate the same result
# comp = conn.raw_sql("""
#                     select mcaldt, tmytm
#                     from crspa.tfz_mth_rf
#                     where mcaldt >= '01/01/2015' and kytreasnox = '2000001'
#                     """)

# # MCALDT Last Quotation Date in the Month
# # TMYTM Monthly Series of Yield to Maturity (TMYLD * 36500)
# # TREASNOX 2000001 (1-month nominal - old columns 2-6)
# # KYTREASNOX 	Num	8	


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
                      where a.date between '01/01/1990' and '12/31/2016'
                      and b.exchcd between 1 and 3
                      """) 
# change variable format to int
crsp_m[['permco','permno','shrcd','exchcd']]=crsp_m[['permco','permno','shrcd','exchcd']].astype(int)

# Line up date to be end of month
crsp_m['date']=pd.to_datetime(crsp_m['date'])
crsp_m['jdate']=crsp_m['date']+MonthEnd(0)


# add delisting return
dlret = conn.raw_sql("""
                     select permno, dlret, dlstdt 
                     from crsp.msedelist
                     """)
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

# lag market cap
crsp2['lme']=crsp2.groupby(['permno'])['me'].shift(1)

# if first permno then use me/(1+retx) to replace the missing value
crsp2['count']=crsp2.groupby(['permno']).cumcount()
crsp2['lme']=np.where(crsp2['count']==0, crsp2['me']/(1+crsp2['retx']), crsp2['lme'])
crsp3=crsp2.copy()
crsp3['wt']=crsp3['lme']


# keeping only records that meet the criteria
ccm3=crsp3.copy()
ccm4=ccm3[(ccm3['wt']>0)& ((ccm3['shrcd']==10) | (ccm3['shrcd']==11))]

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
vwret=ccm4.groupby(['jdate']).apply(wavg, 'retadj','wt').to_frame().reset_index().rename(columns={0: 'WRm'})


# firm count
vwret_n=ccm4.groupby(['jdate'])['retadj'].count().reset_index().rename(columns={'retadj':'n_firms'})

# tranpose
ff_factors=vwret.copy()
ff_nfirms=vwret_n.copy()
ff_factors=ff_factors.rename(columns={'jdate':'date'})
ff_nfirms=ff_nfirms.rename(columns={'jdate':'date'})

###################
# Compare With FF #
###################
_ff = conn.get_table(library='ff', table='factors_monthly')
_ff=_ff[['date','mktrf','rf']]
_ff['date']=_ff['date']+MonthEnd(0)

_ffcomp = pd.merge(_ff, ff_factors[['date','WRm']], how='inner', on=['date'])
_ffcomp['WRmRf']=_ffcomp['WRm']-_ffcomp['rf']

_ffcomp70=_ffcomp[_ffcomp['date']>='01/01/1990']
print(stats.pearsonr(_ffcomp70['mktrf'], _ffcomp70['WRmRf']))


###################
# Visualization of comparison #
###################
#You may wanna change the data range in the future for case by case use
psyear=1990
psmonth=1

peyear=2018
pemonth=12
pday=31

theme='RmRf'
wfactor='WRmRf'
ffactor='mktrf'
pdf1=plt.figure(figsize=(12,8)) 
plt.suptitle("Comparison of Results", fontsize=14)
ax1=plt.subplot(2, 1, 1)
plt.ylabel("Return")
plt.title(theme)
plt.plot(_ffcomp70['date'],_ffcomp70[ffactor],label = ffactor,color='red')
plt.plot(_ffcomp70['date'],_ffcomp70[wfactor], label = wfactor,color='blue')
plt.legend(loc="best")
ax1.set_xlim([datetime.date(psyear, psmonth, pday), datetime.date(peyear, pemonth, pday)])
plt.setp(ax1.get_xticklabels(), visible=False)

_ffcomp60smb=_ffcomp[['date',ffactor,wfactor]]
_ffcomp60smb.set_index(["date"], inplace=True)
ax2=plt.subplot(2, 1, 2)
plt.xlabel('Time(y)')
plt.ylabel("Cumulative Return")
plt.title(theme)
plt.plot((_ffcomp60smb + 1).cumprod() - 1)
ax2.set_xlim([datetime.date(psyear, psmonth, pday), datetime.date(peyear, pemonth, pday)])
plt.legend(loc="best")
plt.show()

#Save pdf
pdfout='FF_Model_RmRf.pdf'
pp = PdfPages(pdfout)
pp.savefig(pdf1)

pp.close()

# ###################
# # output data #
# ###################
dataout='FF_Model_RmRf.xlsx'
writer = pd.ExcelWriter(dataout, engine='xlsxwriter')
_ffcomp.to_excel(writer,sheet_name='ff_factors') 
ff_nfirms.to_excel(writer,sheet_name='ff_nfirms') 
writer.save()

