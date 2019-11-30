#!/usr/bin/env python
# coding: utf-8

# In[38]:



sdate='2013-07-01'
edate='2017-06-30'




import hdf5storage
import pandas as pd
import datetime
from scipy import stats

import hdf5storage
import pandas as pd
import datetime
from scipy import stats
import os
import re

### Scan the dictionary for all datafiles
filepath = "F:\\RA_Fama_French_Factor\\five_factor_model\\HF_Package\\Comparison_PY_MAT_FF\\"
print("start")
if not os.path.exists(filepath):
    print("目录不存在!!")
    os._exit(1)
filenames = os.listdir(filepath)
pyfile=[]
matfile=[]
for data in filenames:
    if '_intraday.csv' in data:
        pyfile.append(data)
    if '_v3.mat' in data:
        matfile.append(data)

#     os.rename(filepath + '\\' + data,filepath + '\\' + newname)
list=matfile
# list=['step4_20130701_20130731_v3.mat','step4_20130801_20130831_v3.mat','step4_20130901_20130930_v3.mat','step4_20131001_20131031_v3.mat',\
#      'step4_20131101_20131130_v3.mat','step4_20131201_20131231_v3.mat','step4_20140101_20140131_v3.mat','step4_20140201_20140228_v3.mat',\
#      'step4_20140301_20140331_v3.mat','step4_20140401_20140430_v3.mat','step4_20140501_20140531_v3.mat','step4_20140601_20140630_v3.mat',\
#     'step4_20140701_20140731_v3.mat','step4_20140801_20140831_v3.mat','step4_20140901_20140930_v3.mat','step4_20141001_20141031_v3.mat',\
#      'step4_20141101_20141130_v3.mat','step4_20141201_20141231_v3.mat','step4_20150101_20150131_v3.mat','step4_20150201_20150228_v3.mat',\
#      'step4_20150301_20150331_v3.mat','step4_20150401_20150430_v3.mat','step4_20150501_20150531_v3.mat','step4_20150601_20150630_v3.mat',\
#       'step4_20150701_20150731_v3.mat','step4_20150801_20150831_v3.mat','step4_20150901_20150930_v3.mat','step4_20151001_20151031_v3.mat',\
#      'step4_20151101_20151130_v3.mat','step4_20151201_20151231_v3.mat','step4_20160101_20160131_v3.mat','step4_20160201_20160229_v3.mat',\
#      'step4_20160301_20160331_v3.mat','step4_20160401_20160430_v3.mat','step4_20160501_20160531_v3.mat','step4_20160601_20160630_v3.mat',\
#     'step4_20160701_20160731_v3.mat','step4_20160801_20160831_v3.mat','step4_20160901_20160930_v3.mat','step4_20161001_20161031_v3.mat',\
#      'step4_20161101_20161130_v3.mat','step4_20161201_20161231_v3.mat','step4_20170101_20170131_v3.mat','step4_20170201_20170228_v3.mat',\
#      'step4_20170301_20170331_v3.mat','step4_20170401_20170430_v3.mat','step4_20170501_20170531_v3.mat','step4_20170601_20170630_v3.mat']
matdatainitial=pd.DataFrame()
for file in list:
    mat = hdf5storage.loadmat(file)
    matdata = pd.DataFrame(mat['return_matrix'],columns = ['Date','Time','matSMB','matHML','matRMW','matCMA','matMOM','matMarket'])
    matdatainitial = pd.concat([matdatainitial,matdata])

CSV_FILE_PATH = filepath
count = 0
pydatainitial=pd.DataFrame()
pylist=pyfile
# pylist=['130701_0813_intraday.csv','130813_0925_intraday.csv','130925_1106_intraday.csv','131106_1221_intraday.csv',\
#      '131221_0204_intraday.csv','140204_0318_intraday.csv','140318_0630_intraday.csv','140701_0815_intraday.csv','140815_0925_intraday.csv','140925_1106_intraday.csv','141106_1219_intraday.csv',\
#      '141219_0204_intraday.csv','150204_0319_intraday.csv','150319_0630_intraday.csv',\
#         '150701_0813_intraday.csv','150813_0925_intraday.csv','150925_1106_intraday.csv','151106_1221_intraday.csv',\
#      '151221_0204_intraday.csv','160204_0318_intraday.csv','160318_0630_intraday.csv','160701_0815_intraday.csv','160815_0927_intraday.csv','160927_1108_intraday.csv','161108_1221_intraday.csv',\
#      '161221_0206_intraday.csv','170206_0321_intraday.csv','170321_0630_intraday.csv']
for file in pylist:
    count += 1
    if count == 1 or count%7==1:
        pydata = pd.read_csv(CSV_FILE_PATH+file)
        DataPc=pydata.copy()
    else:
        pydata = pd.read_csv(CSV_FILE_PATH+file)
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
tick_spacing = 15800
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