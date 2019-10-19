#!/usr/bin/env python
# coding: utf-8

# In[6]:


##########################################
# Fama French Data Grabber to compare firms numbers
# Oct 19 2019
# Created by Xinyu LIU
##########################################

#https://randlow.github.io/posts/finance-economics/pandas-datareader-KF/
#Please refer to this link if you have any further questions.
#jupyter nbconvert --to script five_factor_model\SIZE_OP\Compare_firm_number.ipynb

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

#You can extract all the available datasets from Ken French's website and find that there are 295 of them. We can opt to see all the datasets available.
datasets = get_available_datasets()
print('No. of datasets:{}'.format(len(datasets)))
#datasets # comment out if you want to see all the datasets

##############
#Customize your data selection
##############
#Note:If this is what you are intended to find: '6_Portfolios_ME_OP_2x3', but don't know exactly what it is named, do the following line
#df_me_op_factor = [dataset for dataset in datasets if 'ME' in dataset and 'OP' in dataset and '2x3' in dataset]
#print(df_me_op_factor)

#It is important to check the description of the dataset we access by using the following codes 
Datatoread='6_Portfolios_ME_OP_2x3'
sdate='1982-07-01'
edate='2018-12-31'

ds_factors = web.DataReader(Datatoread,'famafrench',start=sdate,end=edate) # Taking [0] as extracting 1F-F-Research_Data_Factors_2x3')
print('\nKEYS\n{}'.format(ds_factors.keys()))
print('DATASET DESCRIPTION \n {}'.format(ds_factors['DESCR']))
#ds_factors[0].head()
#copy the right dict for later examination
#dfFactor = ds_factors[0].copy()
dfFirm = ds_factors[4].copy()
dfFirm['Offical_total'] = dfFirm.apply(lambda x: x.sum(), axis=1)

#Data processing 
###Not necessary your case
_ff=DataFrame(dfFirm)
_ff=_ff.reset_index()
_ff['Date']=_ff['Date'].apply(lambda x : x.to_timestamp())
_ff['Date']=_ff['Date']+MonthEnd(0)
_ff=_ff[['Date','Offical_total']]
_ff.rename(columns = {'Date':'date'}, inplace = True) 

#suppose I'm reading from excel: return dataframe(sheet_name use direct int). You need to make change on the file name and path
infile='FF_Model_SIZE&BM.xlsx'
strlist = infile.split('.')
stitle=strlist[0]
sheetnum=1
ff_factors=pd.read_excel(infile,sheet_name=sheetnum)
#merge data
_ffcomp = pd.merge(_ff, ff_factors[['date','TOTAL']], how='inner', on=['date'])
print(stats.pearsonr(_ffcomp['TOTAL'], _ffcomp['Offical_total']))

# #you don't need the following comparison if not necessary
pdf1=plt.figure(figsize=(12,8)) 
plt.suptitle("Comparison of Results", fontsize=14)
plt.ylabel("Firm Numbers")
plt.title("Firm")
plt.plot(_ffcomp['date'],_ffcomp['TOTAL'],label = 'TOTAL',color='red')
plt.plot(_ffcomp['date'],_ffcomp['Offical_total'], label = 'Offical_total',color='blue')
plt.legend(loc="best")
plt.show()

#Save pdf
pp = PdfPages(stitle+".pdf")
pp.savefig(pdf1)
pp.close()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




