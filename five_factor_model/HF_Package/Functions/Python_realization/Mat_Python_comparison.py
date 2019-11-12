#!/usr/bin/env python
# coding: utf-8

# In[38]:


import hdf5storage
import pandas as pd
import datetime

mat = hdf5storage.loadmat('step4_20150901_20150930_v3.mat')
matdata = pd.DataFrame(mat['return_matrix'],columns = ['Date','Time','matSMB','matHML','matRMW','matCMA','matMOM','matMarket'])
#Please use HDF reader for matlab v7.3 files

CSV_FILE_PATH = 'F:\\RA_Fama_French_Factor\\five_factor_model\\HF_Package\\Output\\150901_0930_daily_all.csv'
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

from matplotlib.backends.backend_pdf import PdfPages
pp = PdfPages("mat_py_one_month.pdf")
pp.savefig(fig)
pp.savefig(figc)
pp.close()


# In[ ]:





# In[37]:





# In[33]:





# In[36]:


axes


# In[76]:


fig2, axes2 = plt.subplots(nrows=6, ncols=1, figsize=(12,15))
plt.plot((_ffcomp + 1).cumprod() - 1)


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

