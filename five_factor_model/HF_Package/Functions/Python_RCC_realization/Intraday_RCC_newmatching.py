#!/usr/bin/env python
# coding: utf-8

# In[146]:


import pandas as pd
import numpy as np
import DataHub as hub
import datetime

CSV_FILE_PATH = '20000101_0130_daily_all_RCC.csv'
pdata = pd.read_csv(CSV_FILE_PATH)

pdata[['permno']]=pdata[['permno']].astype(int)
pdata['date']=pd.to_datetime(pdata['date'])
# Controlling date range you want to include for intraday calculation
pdstart = pdata.date.iloc[0]
pdend = pdata.date.iloc[3]
ind=(pdata['date'] >= pdstart) & (pdata['date']  <= pdend)
DataPc=pdata[ind]

DataPcnew=DataPc.copy()
#DataPcnew.columns.values.tolist()
DataPcnew=DataPcnew[DataPcnew.prc>0]
DataPcnew['prca']=DataPcnew['prc'].abs()
DataPcnew['openprc']=DataPcnew['openprc'].abs()
DataPcnew['INRet']=DataPcnew['prca']/DataPcnew['openprc']-1;
DataPcnew['ONRet']=(DataPcnew['retadj']+1)/(DataPcnew['INRet']+1)-1;

###################
# Step Two #
###################
DataPcnewcopy=DataPcnew.copy()

DataPcnewcopy=DataPcnewcopy.reset_index(drop=True)
m = DataPcnew.reindex(np.repeat(DataPcnew.index.values, 79), method='ffill')
m['Unnamed: 0'] = m.groupby(['date','permno']).cumcount()
m=m.rename(columns={'Unnamed: 0':'intratime'})
m['intratime']=np.where(m['intratime']<=78,m['intratime'],m['intratime']-79)
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

kd_m=m.copy()
all_kd=pd.DataFrame()
TAQ = hub.Handle.create('TAQ')
for eachday in datelist:
    datain='Matching_test_{}_with_yuxingtable.csv'.format(str(eachday))
    kd = pd.read_csv(datain)
    kd['date']=pd.to_datetime(eachday)
    all_kd = pd.concat([all_kd,kd])
kd_m=pd.merge(kd_m,all_kd,how='left',on=['permno','date'])

all_p5m=pd.DataFrame()
kd_m_p5m=kd_m.copy().sort_values(by=['date','symbol','intratime'])

for eachday in datelist:
    # Extract 5min data from API and save them to p5m dataframe, adding index and date for later merge operation
    eachday_p5m=TAQ.read('Daily5Min', date = eachday)
    eachday_p5m['date'] = pd.to_datetime(eachday)
    eachday_p5m=eachday_p5m.drop(['permno'], axis=1)
    dff = pd.melt(eachday_p5m, id_vars=list(eachday_p5m.columns)[:3], value_vars=list(eachday_p5m.columns)[3:],
             var_name='intratime', value_name='tprice')
    dff = dff.sort_values(by=['date', 'symbol', 'intratime'])
    dff['intratime'] =dff['intratime'].str[1:].astype(int)
    dff.reset_index(drop='true')
    all_p5m = pd.concat([all_p5m,dff])

kd_m_p5m=pd.merge(kd_m_p5m, all_p5m, how='left',on=['date','symbol','intratime'])
kd_m_p5m.reset_index(drop='True',inplace=True)
kd_m_p5m['prca']=np.where((kd_m_p5m['intratime']+1)%79==0,kd_m_p5m['prcadaily'],kd_m_p5m['tprice'])

###################
# Step Three #
###################
ret_kd_m_p5m=kd_m_p5m.copy()
#Clean nomatch
ret_kd_m_p5m=ret_kd_m_p5m.dropna(axis=0,subset=['ticker'])
ret=pd.DataFrame(index=ret_kd_m_p5m.index)

ret['ret']=ret_kd_m_p5m['prca']/ret_kd_m_p5m.groupby(['permno','date'])['prca'].shift(1)-1
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

intra_time=df_bar.copy().iloc[np.tile(np.arange(len(df_bar)), int(len(ff_factors_merged)/79))]
ff_factors_merged['time']=pd.to_timedelta(intra_time.time).reset_index(drop='true')+ff_factors_merged.date
ff_factors_merged.set_index(["time"], inplace=True)
ff_factors_merged.drop(columns=['date','intratime'], inplace=True)

###################
# Matching Description #
###################

threshold=0.1
missingprc=DataPc[DataPc['prc'].isna()]
negativeprc=DataPc[DataPc['prc']<0]
nomatch=kd_m_p5m[kd_m_p5m['ticker'].isna()]
prc_vs_p78=mstep4[mstep4.intratime==78]
overthershold=prc_vs_p78[((prc_vs_p78.prca/prc_vs_p78.tprice).abs()-1)>threshold]

Description=pd.DataFrame(DataPc.groupby(['date'])['permno'].count()).rename(columns={'permno':'stock_in_portfolio'})
Description['missing_prc']=missingprc.groupby(['date'])['permno'].count()
Description['negative_prc']=negativeprc.groupby(['date'])['permno'].count()
Description['nomatch']=(nomatch.groupby(['date'])['permno'].count()/79)
Description['overthershold']=overthershold.groupby(['date'])['permno'].count()
Description=Description.fillna(0)
Description['final_portfolio']=Description.stock_in_portfolio-Description.missing_prc -Description.negative_prc-Description.nomatch
Description['rate_of_matching']=Description.final_portfolio/Description.stock_in_portfolio


###################
#  Plotting  #
###################
import matplotlib
matplotlib.use('pdf')
import matplotlib.pyplot as plt
plt.rcParams.update({'figure.max_open_warning': 0})
plotff=ff_factors_merged.copy()
plotff.reset_index(inplace=True,drop='True')
plt.figure(figsize=(10,15))
theme='FF factors'
plotff.plot(subplots=True, layout=(5,1), figsize=(10,15), title=theme)
plt.ylabel('Value Weighted Return')
plt.xlabel('Time intervals from '+datelist[0] + ' to '+datelist[-1])
simple_returnpdf=datelist[0]+'_to_'+datelist[-1]+'_newmathcing.pdf'
plt.savefig(simple_returnpdf)
            
plt.figure(figsize=(10,15))
((plotff+1).cumprod()-1).plot(subplots=True, layout=(5,1), figsize=(10,15), title=theme)
cum_returnpdf=datelist[0]+'_to_'+datelist[-1]+'_cumulative_newmathcing.pdf'
plt.savefig(cum_returnpdf)

###################
#  Saving results  #
###################
ff_factors_merged.to_csv(datelist[0][2:]+'_'+datelist[-1][4:]+"_daily_all_newmathcing.csv")

###################
#  Additional Charts  #
###################
import matplotlib.backends.backend_pdf
import matplotlib.pyplot as plt
import pandas as pd
table = Description
fig = plt.figure(figsize=(10,5))
ax=plt.subplot(111)
ax.axis('off')
c = table.shape[1]
rownum=len(table)
ax.table(cellText=np.vstack([table.columns, table.values]), cellColours=[['lightgray']*c] + [['none']*c]*rownum, bbox=[0,0,1,1])
descriptionpdf="output.pdf"
pdf = matplotlib.backends.backend_pdf.PdfPages(descriptionpdf)
pdf.savefig(fig)
pdf.close()

plt.rcParams.update({'figure.max_open_warning': 0})
plt.figure(figsize=(10,5))
Description.plot(y='rate_of_matching')
descriptionchart="chart.pdf"
plt.savefig(descriptionchart)

from PyPDF2 import PdfFileMerger
pdfs = [simple_returnpdf, cum_returnpdf, descriptionpdf,descriptionchart]

merger = PdfFileMerger()

for pdf in pdfs:
    merger.append(pdf)

merger.write("result{}_{}.pdf".format(datelist[0],datelist[-1][6:]))
merger.close()


# In[143]:


plt.rcParams.update(plt.rcParamsDefault)


# In[149]:


merger.write("result{}_{}.pdf".format(datelist[0],datelist[-1][6:]))
merger.close()


# In[138]:





# In[139]:





# In[130]:


Description


# In[104]:


fig1 = plt.figure()
Description.plot(x='rate_of_matching')
plt.show()


# In[56]:


kd_m_p5m


# In[39]:



ret_kd_m_p5m_1=kd_m_p5m.copy()
ret_kd_m_p5m_1=ret_kd_m_p5m_1.dropna(axis=0,subset=['ticker'])
ret_1=pd.DataFrame(index=ret_kd_m_p5m_1.index)

ret_1['ret']=ret_kd_m_p5m_1['prca']/ret_kd_m_p5m_1.groupby(['permno','date'])['prca'].shift(1)-1
ret_1['ret']=np.where((ret_kd_m_p5m_1.index)%79==0,ret_kd_m_p5m_1['ONRet'],ret_1['ret'])
ret_1


# In[1]:


import pandas as pd
import numpy as np
import DataHub as hub
import datetime

CSV_FILE_PATH = '1509_daily_all_RCC.csv'
pdata = pd.read_csv(CSV_FILE_PATH)


# In[4]:


pdata


# In[5]:


pdata[['permno']]=pdata[['permno']].astype(int)
pdata['date']=pd.to_datetime(pdata['date'])
# Controlling date range you want to include for intraday calculation
pdstart = pdata.date.iloc[0]
pdend = pdata.date.iloc[0]
ind=(pdata['date'] >= pdstart) & (pdata['date']  <= pdend)
DataPc=pdata[ind]
DataPc

