#!/usr/bin/env python
# coding: utf-8

# In[38]:



sdate='1998-07-01'
edate='2017-06-30'



import hdf5storage
import pandas as pd
import datetime
from scipy import stats
import os
import re

### Scan the dictionary for all datafiles
matfilepath = '/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/outlier/matlab/'
pyfilepath='/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/outlier/Output/'
print("start")

pyfilenames = sorted(os.listdir(pyfilepath))
matfilenames = sorted(os.listdir(matfilepath))
pyfile=[]
matfile=[]
for data in matfilenames:
    if '_v3.mat' in data:
        matfile.append(data)
for data in pyfilenames:
    if '_intraday.csv' in data:
        pyfile.append(data)
list=matfile

matdatainitial=pd.DataFrame()
for file in list:
    mat = hdf5storage.loadmat(matfilepath+file)
    matdata = pd.DataFrame(mat['return_matrix'],columns = ['Date','Time','matSMB','matHML','matRMW','matCMA','matMOM','matMarket'])
    matdatainitial = pd.concat([matdatainitial,matdata])

count = 0
pydatainitial=pd.DataFrame()
pylist=pyfile

for file in pylist:
    count += 1
    if count == 1 or count%7==1:
        pydata = pd.read_csv(pyfilepath+file)
        DataPc=pydata.copy()
    else:
        pydata = pd.read_csv(pyfilepath+file)
        DataPc=pydata.copy()
        DataPc=DataPc.iloc[79:,:]
    DataPc=DataPc.set_index('time')
    pydatainitial = pd.concat([pydatainitial,DataPc])

if len(matdatainitial)==len(pydatainitial):
    matdatainitial=matdatainitial.set_index(pydatainitial.index)
    comparison=matdatainitial.join(pydatainitial)
    comparison.drop(columns=['Date','Time'],inplace=True)

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import numpy as np
tick_spacing = 15800*2*3
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

cormatrix=pd.DataFrame(mydata, index=['HML','RMW','CMA','MOM','Rm','SMB'], columns=['corrcoefficient mat&py','p value'])
cormatrixcopy=cormatrix.reset_index()

figt = plt.figure(figsize=(10,10))
ax = plt.subplot(111)
ax.axis('off')
ax.table(cellText=cormatrixcopy.values, colLabels=cormatrixcopy.columns, bbox=[0,0,1,1])





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

test=((mat_py_ff+1).cumprod()-1)
figdaily, axesd = plt.subplots(nrows=6, ncols=1, figsize=(12,15))

for x in axesd:
    x.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
test.plot(y=['matHML','HML','ff_HML'],ax=axesd[0])
test.plot(y=['matRMW','RMW','ff_RMW'],ax=axesd[1])
test.plot(y=['matCMA','CMA','ff_CMA'],ax=axesd[2])
test.plot(y=['matMOM','MOM','ff_MOM'],ax=axesd[3])
test.plot(y=['matMarket','Rm','ff_Mkt-RF'],ax=axesd[4])
test.plot(y=['matSMB','SMB','ff_SMB'],ax=axesd[5])

plt.show()

corhml_mf=stats.pearsonr(mat_py_ff['matHML'], mat_py_ff['ff_HML'])
corrmw_mf=stats.pearsonr(mat_py_ff['matRMW'], mat_py_ff['ff_RMW'])
corcma_mf=stats.pearsonr(mat_py_ff['matCMA'], mat_py_ff['ff_CMA'])
cormom_mf=stats.pearsonr(mat_py_ff['matMOM'], mat_py_ff['ff_MOM'])
cormkt_mf=stats.pearsonr(mat_py_ff['matMarket'], mat_py_ff['ff_Mkt-RF'])
corsmb_mf=stats.pearsonr(mat_py_ff['matSMB'], mat_py_ff['ff_SMB'])
mydata=[corhml_mf,corrmw_mf,corcma_mf,cormom_mf,cormkt_mf,corsmb_mf]

cormatrix_mf=pd.DataFrame(mydata, index=['HML','RMW','CMA','MOM','Rm','SMB'], columns=['corcoefficient between matlab and ff','p value'])

corhml_pf=stats.pearsonr(mat_py_ff['HML'], mat_py_ff['ff_HML'])
corrmw_pf=stats.pearsonr(mat_py_ff['RMW'], mat_py_ff['ff_RMW'])
corcma_pf=stats.pearsonr(mat_py_ff['CMA'], mat_py_ff['ff_CMA'])
cormom_pf=stats.pearsonr(mat_py_ff['MOM'], mat_py_ff['ff_MOM'])
cormkt_pf=stats.pearsonr(mat_py_ff['Rm'], mat_py_ff['ff_Mkt-RF'])
corsmb_pf=stats.pearsonr(mat_py_ff['SMB'], mat_py_ff['ff_SMB'])
mydata=[corhml_pf,corrmw_pf,corcma_pf,cormom_pf,cormkt_pf,corsmb_pf]

cormatrix_pf=pd.DataFrame(mydata, index=['HML','RMW','CMA','MOM','Rm','SMB'], columns=['corcoefficient between python and ff','p value'])

cormatrix_pf_mf=pd.merge(cormatrix_pf[['corcoefficient between python and ff']],cormatrix_mf[['corcoefficient between matlab and ff']],left_index=True,right_index=True)
cormatrix_pf_mf.reset_index(inplace=True)

figtm = plt.figure(figsize=(10,10))
ax = plt.subplot(111)
ax.axis('off')
ax.table(cellText=cormatrix_pf_mf.values, colLabels=cormatrix_pf_mf.columns, bbox=[0,0,1,1])

from matplotlib.backends.backend_pdf import PdfPages
pp = PdfPages(f"{sdate[2:]}_{edate[2:]}mat_py_ff.pdf")
pp.savefig(fig)
pp.savefig(figc)
pp.savefig(figt)
pp.savefig(figdaily)
pp.savefig(figtm)

pp.close()

