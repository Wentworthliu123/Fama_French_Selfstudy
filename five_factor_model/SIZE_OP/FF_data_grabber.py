##########################################
# Fama French Data Grabber
# Oct 17 2019
# Created by Xinyu LIU
##########################################

#https://randlow.github.io/posts/finance-economics/pandas-datareader-KF/
#Please refer to this link if you have any further questions.

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
ds_factors = web.DataReader('F-F_Research_Data_5_Factors_2x3','famafrench',start='1982-07-01',end='2018-12-31') # Taking [0] as extracting 1F-F-Research_Data_Factors_2x3')
print('\nKEYS\n{}'.format(ds_factors.keys()))
print('DATASET DESCRIPTION \n {}'.format(ds_factors['DESCR']))
#ds_factors[0].head()
#copy the right dict for later examination
dfFactor = ds_factors[0].copy()

#Data processing 
###Not necessary your case
dfFactor.head()
_ff=DataFrame(dfFactor)
_ff=_ff.reset_index()
_ff.dtypes
_ff['Date']=_ff['Date'].apply(lambda x : x.to_timestamp())
_ff['Date']=_ff['Date']+MonthEnd(0)
_ff=_ff[['Date','RMW']]
_ff.rename(columns = {'Date':'date'}, inplace = True) 

#suppose I'm reading from excel: return dataframe. You need to make change on the file name and path
ff_factors=pd.read_excel('FF_Model_RMW8018.xlsx')
#merge data
_ffcomp = pd.merge(_ff, ff_factors[['date','WRMW']], how='inner', on=['date'])
print(stats.pearsonr(_ffcomp['WRMW'], _ffcomp['RMW']))

# #you don't need the following comparison if not necessary
# plt.figure(figsize=(12,8)) 
# plt.suptitle("Comparison of Results", fontsize=14)
# plt.ylabel("Return")
# plt.title("RMW")
# plt.plot(_ffcomp['date'],_ffcomp['RMW'],label = 'RMW',color='red')
# plt.plot(_ffcomp['date'],_ffcomp['WRMW'], label = 'WRMW',color='blue')
# plt.legend(loc="best")

# _ffcomp.set_index(["date"], inplace=True)
# plt.figure(figsize=(12,8)) 
# plt.suptitle("Comparison of Results", fontsize=14)
# plt.ylabel("Cumulative Return")
# plt.title("RMW")
# plt.plot((_ffcomp + 1).cumprod() - 1)
# plt.legend(loc="best")