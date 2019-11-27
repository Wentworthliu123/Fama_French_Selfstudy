#!/usr/bin/env python
# coding: utf-8

# In[2]:


#!/usr/bin/env python
# coding: utf-8

# In[38]:



sdate='2015-09-01'
edate='2015-09-30'




import hdf5storage
import pandas as pd
import datetime
from scipy import stats

mat = hdf5storage.loadmat('step4_20150901_20150930_v3.mat')
matdata = pd.DataFrame(mat['return_matrix'],columns = ['Date','Time','matSMB','matHML','matRMW','matCMA','matMOM','matMarket'])
#Please use HDF reader for matlab v7.3 files

CSV_FILE_PATH = 'F:\\RA_Fama_French_Factor\\five_factor_model\\HF_Package\\Comparison_PY_MAT_FF\\150901_0930_daily_all_newmathcing_smb.csv'
pydata = pd.read_csv(CSV_FILE_PATH)

DataPc=pydata.copy()
date=21
DataPc=DataPc.iloc[:date*79,:]
DataPc=DataPc.set_index('time')
matdata=matdata.set_index(DataPc.index)
comparison=matdata.join(DataPc)
comparison.drop(columns=['Date','Time'],inplace=True)

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import numpy as np
tick_spacing = 395
# gca stands for 'get current axis'
# comparison.reset_index(inplace=True)
fig, axes = plt.subplots(nrows=6, ncols=1, figsize=(12,15))

for x in axes:
    x.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
comparison.plot(y=['matHML','HML'],ax=axes[0])
comparison.plot(y=['matRMW','RMW'],ax=axes[1])
comparison.plot(y=['matCMA','CMA'],ax=axes[2])
comparison.plot(y=['matMOM','MOM'],ax=axes[3])
comparison.plot(y=['matMarket','Rm'],ax=axes[4])
comparison.plot(y=['matSMB','SMB'],ax=axes[5])

figc, axesc = plt.subplots(nrows=6, ncols=1, figsize=(12,15))
comparisoncum=(comparison+1).cumprod()-1

for x in axesc:
    x.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
comparisoncum.plot(y=['matHML','HML'],ax=axesc[0])
comparisoncum.plot(y=['matRMW','RMW'],ax=axesc[1])
comparisoncum.plot(y=['matCMA','CMA'],ax=axesc[2])
comparisoncum.plot(y=['matMOM','MOM'],ax=axesc[3])
comparisoncum.plot(y=['matMarket','Rm'],ax=axesc[4])
comparisoncum.plot(y=['matSMB','SMB'],ax=axesc[5])



plt.show()

corhml=stats.pearsonr(comparison['matHML'], comparison['HML'])
corrmw=stats.pearsonr(comparison['matRMW'], comparison['RMW'])
corcma=stats.pearsonr(comparison['matCMA'], comparison['CMA'])
cormom=stats.pearsonr(comparison['matMOM'], comparison['MOM'])
cormkt=stats.pearsonr(comparison['matMarket'], comparison['Rm'])
corsmb=stats.pearsonr(comparison['matSMB'], comparison['SMB'])
mydata=[corhml,corrmw,corcma,cormom,cormkt,corsmb]

cormatrix=pd.DataFrame(mydata, index=['HML','RMW','CMA','MOM','Rm','SMB'], columns=['correlation coefficient','p value'])


figt = plt.figure(figsize=(10,10))
ax = plt.subplot(111)
ax.axis('off')
ax.table(cellText=cormatrix.values, colLabels=cormatrix.columns, bbox=[0,0,1,1])


from matplotlib.backends.backend_pdf import PdfPages
pp = PdfPages("mat_py_one_month.pdf")
pp.savefig(fig)
pp.savefig(figc)
pp.savefig(figt)
pp.close()


import pandas_datareader.data as web  # module for reading datasets directly from the web
#pip install pandas-datareader (in case you haven't install this package)
from pandas_datareader.famafrench import get_available_datasets

Datatoreadff='F-F_Research_Data_5_Factors_2x3_daily'


ds_factorsff = web.DataReader(Datatoreadff,'famafrench',start=sdate,end=edate) # Taking [0] as extracting 1F-F-Research_Data_Factors_2x3')
print('\nKEYS\n{}'.format(ds_factorsff.keys()))
print('DATASET DESCRIPTION \n {}'.format(ds_factorsff['DESCR']))

#ds_factors[0].head()
#copy the right dict for later examination
dfFactorff = ds_factorsff[0].copy()/100
#dfFirm = ds_factors[4].copy()
#dfFirm['Offical_total'] = dfFirm.apply(lambda x: x.sum(), axis=1)
_ff=pd.DataFrame(dfFactorff)
_ff=_ff.reset_index()
#Data processing 
###Not necessary your case
_ff=pd.DataFrame(dfFactorff)
_ff.columns=['ff_'+col for col in _ff.columns]
# _ff=_ff.reset_index()


Datatoread='6_Portfolios_ME_Prior_12_2_Daily'
ds_factors = web.DataReader(Datatoread,'famafrench',start=sdate,end=edate) # Taking [0] as extracting 1F-F-Research_Data_Factors_2x3')
print('\nKEYS\n{}'.format(ds_factors.keys()))
print('DATASET DESCRIPTION \n {}'.format(ds_factors['DESCR']))
#ds_factors[0].head()
#copy the right dict for later examination
dfFactor = ds_factors[0].copy()/100
#dfFirm = ds_factors[4].copy()
#dfFirm['Offical_total'] = dfFirm.apply(lambda x: x.sum(), axis=1)
mom_ff=pd.DataFrame(dfFactor)
# mom_ff=mom_ff.reset_index()
mom_ff['ff_MOM']=(mom_ff['BIG HiPRIOR']+mom_ff['SMALL HiPRIOR']-mom_ff['SMALL LoPRIOR']-mom_ff['BIG LoPRIOR'])/2
mom_ff=mom_ff[['ff_MOM']]
# mom_ff.rename(columns = {'Date':'date'}, inplace = True) 
_ff=pd.merge(_ff,mom_ff,left_index=True, right_index=True)

comparisoncopy=(comparison+1).reset_index()
comparisoncopy.time=pd.to_datetime(comparisoncopy.time)

comparisoncopy_daily = comparisoncopy.resample('D', on='time').prod()

mat_py=comparisoncopy_daily[comparisoncopy_daily.Rm!=1]-1
mat_py_ff=pd.merge(mat_py,_ff,left_index=True, right_index=True)
((mat_py_ff+1).cumprod()-1).plot(figsize=(15,10))





