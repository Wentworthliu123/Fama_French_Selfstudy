
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import stock
import datetime

CSV_FILE_PATH = '/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/1509_daily_all.csv'
pdata = pd.read_csv(CSV_FILE_PATH)

pdata[['permno','shrcd','exchcd']]=pdata[['permno','shrcd','exchcd']].astype(int)
pdata['date']=pd.to_datetime(pdata['date'])
# Controlling date range you want to include for intraday calculation
pdstart = pdata.date.iloc[0]
pdend = pdata.date.iloc[-1]
ind=(pdata['date'] >= pdstart) & (pdata['date']  <= pdend)
DataPc=pdata[ind]

DataPcnew=DataPc.copy()
#DataPcnew.columns.values.tolist()
DataPcnew['prca']=DataPcnew['prc'].abs()
DataPcnew['openprc']=DataPcnew['openprc'].abs()
DataPcnew['lprc']=DataPcnew.groupby(['permno'])['prca'].shift(1)
DataPcnew['INRet']=DataPcnew['prca']/DataPcnew['openprc']-1;
DataPcnew['ONRet']=(DataPcnew['retadj']+1)/(DataPcnew['INRet']+1)-1;

###################
# Step Two #
###################
DataPcnewcopy=DataPcnew.copy()

DataPcnewcopy=DataPcnewcopy.reset_index(drop=True)
m = DataPcnew.reindex(np.repeat(DataPcnew.index.values, 79), method='ffill')
m['Unnamed: 0'] = m.groupby(['date','permno']).cumcount()
m=m.rename(columns={'Unnamed: 0':'count'})
m.index = range(len(m))
m['prcadaily'] = m['prca']

# Create datatime ticker from df_bar
df_bar=pd.DataFrame()
df_bar[['date','time']] = pd.date_range('09:30', '16:00', freq= '5min').to_series().apply(
            lambda x: pd.Series([i for i in str(x).split(" ")]))
df_bar.index = range(len(df_bar))
datecomplete = list(map(lambda x: x.strftime("%Y%m%d"),DataPcnew.date))
datelist=list(set(datecomplete))
datelist.sort()
# Getting date time list
# def dateRange(beginDate, endDate):
#     dates = []
#     dt = beginDate
#     date = str()
#     while dt <= endDate:
#         date = dt.strftime("%Y%m%d")
#         dates.append(date)
#         dt = dt + datetime.timedelta(1)
#     return dates

# datelist = dateRange(beginDate=pdstart, endDate=pdend)

path='/project2/dachxiu/hf_ff_project/index_day/'
kd_m=m.copy()
all_kd=pd.DataFrame()
for eachday in datelist:
    fd=(path+eachday+'.txt')
    #kd is the key link dataframe that match TAQ symbol with CRSP permno
    k = np.loadtxt(fd, delimiter=' ',dtype=str)
    kd=pd.DataFrame(k)
    kd.iloc[:,1]=kd.iloc[:,1].astype(int)
    kd=kd.rename(columns={0:'symbol',1:'permno'})
    kd['date']=pd.to_datetime(eachday)
    all_kd = pd.concat([all_kd,kd])
kd_m=pd.merge(kd_m,all_kd,how='left',on=['permno','date'])

all_p5m=pd.DataFrame()
kd_m_p5m=kd_m.copy().sort_values(by=['date','symbol','count'])
allsymbol=np.unique(kd_m.symbol)
for eachday in datelist:
    # Extract 5min data from API and save them to p5m dataframe, adding index and date for later merge operation
    eachday_p5m=pd.DataFrame()
    for tSymbol in allsymbol:
        price5min = stock.Query5Min(eachday, tSymbol)
        p5m=pd.DataFrame(list(price5min)).T
        p5m.columns=['intratime','tprice','mquote']
        p5m['symbol']=tSymbol
        p5m.intratime=df_bar.time
        p5m.reset_index(inplace=True)
        eachday_p5m = pd.concat([eachday_p5m,p5m])
    eachday_p5m['date'] = pd.to_datetime(eachday)
    all_p5m = pd.concat([all_p5m,eachday_p5m])
all_p5m.rename(columns={'index':'count'},inplace=True)
kd_m_p5m=pd.merge(kd_m_p5m, all_p5m, how='left',on=['date','symbol','count'])
kd_m_p5m=kd_m_p5m.sort_values(by=['date','permno','count'])
kd_m_p5m.reset_index(drop='True',inplace=True)
kd_m_p5m['prca']=np.where((kd_m_p5m['count']+1)%79==0,kd_m_p5m['prcadaily'],kd_m_p5m['tprice'])

###################
# Step Three #
###################
ret_kd_m_p5m=kd_m_p5m.copy()
ret=pd.DataFrame(index=kd_m_p5m.index)
ret['ret']=ret_kd_m_p5m['prca']/ret_kd_m_p5m['prca'].shift(1)-1
ret['ret']=np.where((ret_kd_m_p5m.index)%79==0,ret_kd_m_p5m['ONRet'],ret['ret'])
ret['1+ret']=1+ret['ret']
ret['permno']=ret_kd_m_p5m['permno']
ret['cumret']=ret.groupby(['permno'])['1+ret'].cumprod()
ret['lcumret']=ret.groupby(['permno'])['cumret'].shift(1).fillna(1)

ret_kd_m_p5m['retadj']=ret['ret']
ret_kd_m_p5m['wt'] = ret['lcumret']*ret_kd_m_p5m['wt']

###################
# Step Four #
###################
mstep4=ret_kd_m_p5m.copy()
def wavg(group, avg_name, weight_name):
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return np.nan
    
# Rm factor #
vwret_rm=mstep4.groupby(['date','intratime']).apply(wavg, 'retadj','wt').to_frame().reset_index().rename(columns={0: 'Rm'})
vwret_n=mstep4.groupby(['date','intratime'])['retadj'].count().reset_index().rename(columns={'retadj':'n_firms'})
ff_factors=vwret_rm.copy()
ff_nfirms=vwret_n.copy()

# Value factor #
mstep4_bm=mstep4[mstep4['nonmissport_bm']==1]
vwret_bm=mstep4_bm.groupby(['date','intratime','szport','bmport']).apply(wavg, 'retadj','wt').to_frame().reset_index().rename(columns={0: 'vwret'})
vwret_bm['sbport']=vwret_bm['szport']+vwret_bm['bmport']
ff_factors_bm=vwret_bm.pivot_table(index=['date','intratime'], columns='sbport', values='vwret').reset_index()
ff_factors_bm['HMLH']=(ff_factors_bm['BH']+ff_factors_bm['SH'])/2
ff_factors_bm['HMLL']=(ff_factors_bm['BL']+ff_factors_bm['SL'])/2
ff_factors_bm['HML'] = ff_factors_bm['HMLH']-ff_factors_bm['HMLL']
ff_factors_bm=ff_factors_bm[['date','intratime','HML']]

# Profit factor #
mstep4_op=mstep4[mstep4['nonmissport_rw']==1]
vwret_op=mstep4_op.groupby(['date','intratime','szport','rwport']).apply(wavg, 'retadj','wt').to_frame().reset_index().rename(columns={0: 'vwret'})
vwret_op['srport']=vwret_op['szport']+vwret_op['rwport']
ff_factors_op=vwret_op.pivot_table(index=['date','intratime'], columns='srport', values='vwret').reset_index()
ff_factors_op['RMWR']=(ff_factors_op['BR']+ff_factors_op['SR'])/2
ff_factors_op['RMWW']=(ff_factors_op['BW']+ff_factors_op['SW'])/2
ff_factors_op['RMW'] = ff_factors_op['RMWR']-ff_factors_op['RMWW']
ff_factors_op=ff_factors_op[['date','intratime','RMW']]

# Investment factor #
mstep4_ca=mstep4[mstep4['nonmissport_ca']==1]
vwret_ca=mstep4_ca.groupby(['date','intratime','szport','caport']).apply(wavg, 'retadj','wt').to_frame().reset_index().rename(columns={0: 'vwret'})
vwret_ca['scport']=vwret_ca['szport']+vwret_ca['caport']
ff_factors_ca=vwret_ca.pivot_table(index=['date','intratime'], columns='scport', values='vwret').reset_index()
ff_factors_ca['CMAC']=(ff_factors_ca['BC']+ff_factors_ca['SC'])/2
ff_factors_ca['CMAA']=(ff_factors_ca['BA']+ff_factors_ca['SA'])/2
ff_factors_ca['CMA'] = ff_factors_ca['CMAC']-ff_factors_ca['CMAA']
ff_factors_ca=ff_factors_ca[['date','intratime','CMA']]

# Momentum factor #
mstep4_mom=mstep4[mstep4['nonmissport_mom']==1]
vwret_mom=mstep4_mom.groupby(['date','intratime','szport','momport']).apply(wavg, 'retadj','wt').to_frame().reset_index().rename(columns={0: 'vwret'})
vwret_mom['smport']=vwret_mom['szport']+vwret_mom['momport']
ff_factors_mom=vwret_mom.pivot_table(index=['date','intratime'], columns='smport', values='vwret').reset_index()
ff_factors_mom['MOMH']=(ff_factors_mom['BH']+ff_factors_mom['SH'])/2
ff_factors_mom['MOML']=(ff_factors_mom['BL']+ff_factors_mom['SL'])/2
ff_factors_mom['MOM'] = ff_factors_mom['MOMH']-ff_factors_mom['MOML']
ff_factors_mom=ff_factors_mom[['date','intratime','MOM']]

from functools import reduce
data_frames = [ff_factors, ff_factors_bm, ff_factors_op, ff_factors_ca, ff_factors_mom]
ff_factors_merged = reduce(lambda  left,right: pd.merge(left,right,on=['date','intratime'], how='inner'), data_frames)
ff_factors_merged['time']=pd.to_timedelta(ff_factors_merged.intratime)+ff_factors_merged.date
ff_factors_merged.set_index(["time"], inplace=True)
ff_factors_merged.drop(columns=['date','intratime'], inplace=True)

###################
#  Plotting  #
###################
import matplotlib
matplotlib.use('pdf')
import matplotlib.pyplot as plt
plotff=ff_factors_merged.copy()
plotff.reset_index(inplace=True,drop='True')
plt.figure(figsize=(10,15))
theme='FF factors'
plotff.plot(subplots=True, layout=(5,1), figsize=(10,15), title=theme)
plt.ylabel('Value Weighted Return')
plt.xlabel('Time intervals from '+datelist[0] + ' to '+datelist[-1])
plt.savefig(datelist[0]+'_to_'+datelist[-1]+'.pdf')
            
plt.figure(figsize=(10,15))
((plotff+1).cumprod()-1).plot(subplots=True, layout=(5,1), figsize=(10,15), title=theme)
plt.savefig(datelist[0]+'_to_'+datelist[-1]+'_cumulative.pdf')

###################
#  Saving results  #
###################
ff_factors_merged.to_csv(datelist[0][2:]+'_'+datelist[-1][4:]+"_daily_all.csv")


