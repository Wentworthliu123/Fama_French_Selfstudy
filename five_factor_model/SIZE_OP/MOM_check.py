#!/usr/bin/env python
# coding: utf-8

# In[3]:


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
Datatoread='6_Portfolios_ME_Prior_12_2'
sdate='1996-07-31'
edate='2018-12-31'

ds_factors = web.DataReader(Datatoread,'famafrench',start=sdate,end=edate) # Taking [0] as extracting 1F-F-Research_Data_Factors_2x3')
print('\nKEYS\n{}'.format(ds_factors.keys()))
print('DATASET DESCRIPTION \n {}'.format(ds_factors['DESCR']))
#ds_factors[0].head()
#copy the right dict for later examination
dfFactor = ds_factors[0].copy()/100
#dfFirm = ds_factors[4].copy()
#dfFirm['Offical_total'] = dfFirm.apply(lambda x: x.sum(), axis=1)
_ff=DataFrame(dfFactor)
_ff=_ff.reset_index()
#Data processing 
###Not necessary your case
_ff=DataFrame(dfFactor)
_ff=_ff.reset_index()
factor='HML'
wfactor='WHML'
_ff['Date']=_ff['Date'].apply(lambda x : x.to_timestamp())
_ff['Date']=_ff['Date']+MonthEnd(0)
_ff[factor]=(_ff['BIG HiPRIOR']+_ff['SMALL HiPRIOR']-_ff['SMALL LoPRIOR']-_ff['BIG LoPRIOR'])/2
_ff=_ff[['Date',factor]]
_ff.rename(columns = {'Date':'date'}, inplace = True) 

#suppose I'm reading from excel: return dataframe(sheet_name use direct int). You need to make change on the file name and path
infile='F:\\RA_Fama_French_Factor\\five_factor_model\\SIZE_MOM\\FF_Model_MOM8018.xlsx'
strlist = infile.split('.')
stitle=strlist[0]
sheetnum=0
ff_factors=pd.read_excel(infile,sheet_name=sheetnum)

#merge data
_ffcomp = pd.merge(_ff, ff_factors[['date',wfactor]], how='inner', on=['date'])
print(stats.pearsonr(_ffcomp[wfactor], _ffcomp[factor]))

# #you don't need the following comparison if not necessary
pdf1=plt.figure(figsize=(12,4)) 
plt.suptitle("Comparison of Results", fontsize=14)
plt.ylabel("Return")
plt.title(factor)
plt.plot(_ffcomp['date'],_ffcomp[factor],label = 'FF_'+factor,color='red')
plt.plot(_ffcomp['date'],_ffcomp[wfactor], label = 'Rep_'+wfactor,color='blue')
plt.legend(loc="best")

pdf2=_ffcomp.set_index(["date"], inplace=True)
plt.figure(figsize=(12,4)) 
plt.suptitle("Comparison of Results", fontsize=14)
plt.ylabel("Cumulative Return")
plt.title(factor)
plt.plot((_ffcomp + 1).cumprod() - 1)
plt.legend(loc="best")

pp = PdfPages(stitle+".pdf")
pp.savefig(pdf1)
pp.savefig(pdf2)
pp.close()


# In[ ]:





# In[10]:





# In[ ]:




