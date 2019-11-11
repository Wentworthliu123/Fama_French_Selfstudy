#!/usr/bin/env python
# coding: utf-8

# In[2]:



import hdf5storage
import pandas as pd


dataname='alldata_1316_fixed.mat'
mat = hdf5storage.loadmat(dataname)
pdata = pd.DataFrame(mat['Data'],columns = ['DATE','PERMNO','VOL','SHROUT','retadj','LME','ret','prca','OPENPRC','divamt','facpra','facshr','SHRCD','EXCHCD','DLRET','DLPRC','DISTCD','PERMCO','weight_port','lprc','ME','ticker_idx','SIZEPORT','BTMPORT','OPPORT','INVPORT','RRGRP'])
# startdate = 20150901
# enddate = 20150901
# ind=(pdata['DATE'] >= startdate) & (pdata['DATE']  <= enddate)
DataPc=pdata
entry=[1,2,19,8,5,10,11,12,6,15,16,17,20,21,23,26,9]
#entry =  [1,2,19,8,5,9,10,11,12,6,15,16,17,20,21,22,23,24,25,26,27]
entryew=[i-1 for i in entry]
DataPcnew=DataPc.iloc[:,entryew].copy()
#DataPcnew.columns.values.tolist()
DataPcnew['prca']=DataPcnew['prca'].abs()
DataPcnew['lprc']=DataPcnew['lprc'].abs()



a = DataPcnew['DATE'].astype(str).copy()
a =a.map(lambda x:x[0:4]+'-'+x[4:6]+'-'+x[6:8])
DataPcnew['datadate']=pd.to_datetime(a)
DataPcnew
import numpy as np
DataPcnew['nonmissport']=np.where((DataPcnew['INVPORT']!=''), 1, 0)
def sz_bucket(row):
    if row['SIZEPORT']==np.nan:
        value=''
    elif row['SIZEPORT']==1:
        value='S'
    else:
        value='B'
    return value

def rw_bucket(row):
    if row['INVPORT']==1:
        value = 'C'
    elif row['INVPORT']==2:
        value='M'
    elif row['INVPORT']==3:
        value='A'
    else:
        value=''
    return value

# assign size portfolio
DataPcnew['szport']=np.where((DataPcnew['ME']>0), DataPcnew.apply(sz_bucket, axis=1), '')
# assign book-to-market portfolio
DataPcnew['rwport']=np.where((DataPcnew['ME']>0), DataPcnew.apply(rw_bucket, axis=1), '')


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
vwret=DataPcnew.groupby(['datadate','szport','rwport']).apply(wavg, 'retadj','weight_port').to_frame().reset_index().rename(columns={0: 'vwret'})
vwret['sbport']=vwret['szport']+vwret['rwport']

# # firm count
# vwret_n=ccm4.groupby(['jdate','szport','rwport'])['retadj'].count().reset_index().rename(columns={'retadj':'n_firms'})
# vwret_n['sbport']=vwret_n['szport']+vwret_n['rwport']

# tranpose
ff_factors=vwret.pivot(index='datadate', columns='sbport', values='vwret').reset_index()
# ff_nfirms=vwret_n.pivot(index='jdate', columns='sbport', values='n_firms').reset_index()

# create SMB and HML factors
ff_factors['WW']=(ff_factors['BC']+ff_factors['SC'])/2
ff_factors['WR']=(ff_factors['BA']+ff_factors['SA'])/2
ff_factors['matCMA'] = ff_factors['WR']-ff_factors['WW']

# ff_factors['WB']=(ff_factors['BW']+ff_factors['BM']+ff_factors['BR'])/3
# ff_factors['WS']=(ff_factors['SW']+ff_factors['SM']+ff_factors['SR'])/3
# ff_factors['WSMB'] = ff_factors['WS']-ff_factors['WB']
ff_factors=ff_factors.rename(columns={'datadate':'date'})
mat=ff_factors[['date','matCMA']]


CSV_FILE_PATH = 'F:\\RA_Fama_French_Factor\\five_factor_model\\SIZE_CMA\\FF_Model_CMA8018.xlsx'
pydata = pd.read_excel(CSV_FILE_PATH, index_col=0)
start=pd.to_datetime('2016-12-30')
end=pd.to_datetime('2013-07-01')
py=pydata[(pydata.date<=start) & (pydata.date>= end)]
py=py[['date','WCMA']].reset_index(drop='true')

comparison = pd.merge(mat,py,how='inner',on='date')



import pandas_datareader.data as web  # module for reading datasets directly from the web
#pip install pandas-datareader (in case you haven't install this package)
from pandas_datareader.famafrench import get_available_datasets
import pickleshare
import pandas as pd
from pandas.core.frame import DataFrame
import numpy as np
import datetime as dt
import psycopg2 
import matplotlib.pyplot as plt
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from scipy import stats
from matplotlib.backends.backend_pdf import PdfPages
factor='matCMA'
wfactor='WCMA'
print(stats.pearsonr(comparison[factor], comparison[wfactor]))


pdf1=plt.figure(figsize=(12,4)) 
plt.suptitle("Comparison of Results", fontsize=14)
plt.ylabel("Return")
plt.title(factor)
plt.plot(comparison['date'],comparison[factor],label = factor,color='red')
plt.plot(comparison['date'],comparison[wfactor], label = 'py_'+wfactor,color='blue')
plt.legend(loc="best")


# In[11]:





# In[8]:





# In[3]:





# In[4]:





# In[16]:


ff_factors


# In[20]:





# In[23]:





# In[27]:





# In[29]:





# In[6]:





# In[7]:





# In[8]:


pp = PdfPages(factor+".pdf")
pp.savefig(pdf1)
pp.close()

