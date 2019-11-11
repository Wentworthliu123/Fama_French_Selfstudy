#!/usr/bin/env python
# coding: utf-8

# In[80]:


import hdf5storage
import pandas as pd
import datetime

mat = hdf5storage.loadmat('step4_20150901_20150901_v3.mat')
matdata = pd.DataFrame(mat['return_matrix'],columns = ['Date','Time','matSMB','matHML','matRMW','matCMA','matMOM','matMarket'])
#Please use HDF reader for matlab v7.3 files

CSV_FILE_PATH = 'F:\\RA_Fama_French_Factor\\five_factor_model\\HF_Package\\Output\\150901_0930_daily_all.csv'
pydata = pd.read_csv(CSV_FILE_PATH)

DataPc=pydata.copy()
date=1
DataPc=DataPc.iloc[:date*79,:]
DataPc=DataPc.set_index('time')
matdata=matdata.set_index(DataPc.index)
comparison=matdata.join(DataPc)

import matplotlib.pyplot as plt
import pandas as pd

# gca stands for 'get current axis'
# comparison.reset_index(inplace=True)
fig, axes = plt.subplots(nrows=5, ncols=1, figsize=(12,15))

comparison.plot(y=['matHML','HML'],ax=axes[0])
comparison.plot(y=['matRMW','RMW'],ax=axes[1])
comparison.plot(y=['matCMA','CMA'],ax=axes[2])
comparison.plot(y=['matMOM','MOM'],ax=axes[3])
comparison.plot(y=['matMarket','Rm'],ax=axes[4])


plt.show()


# In[93]:





# In[ ]:





# In[76]:





# In[77]:





# In[125]:


pydata['time']=pd.to_datetime(pydata['time'])
k = pydata.set_index('time')


# In[16]:





# In[133]:


import numpy as np
m=np.exp((np.log(k['CMA']+1)).resample('D').sum())-1


# In[134]:


m


# In[ ]:


0.00111511
-0.007213294
0.006276554
-0.004552415
-0.002808245
0.001228686
-0.007219835
-0.002382892
-0.001263564
0.000657144
0.002090389
-0.004597385
-0.005002377
0.002341202
0.000559484
-0.002348639
0.001440388
0.006268504
0.007529305
0.007160935
-0.003314748

