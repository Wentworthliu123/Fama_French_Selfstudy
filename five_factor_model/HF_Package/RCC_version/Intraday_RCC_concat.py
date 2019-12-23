#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import datetime
import os.path

portfoliopath = '/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/outlier/Portfolio'
outlierpath='/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/outlier/Outlier'
taqoutpath='/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/outlier/TAQdata'
matchoutpath='/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/outlier/TAQmatch'
outpath='/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/outlier/Output'

outliertotal=pd.DataFrame()
factortotal=pd.DataFrame()    
calstart=1998
calend=2017
for year in range(calstart, calend):

    CSV_FILE_PATH = f'{str(year)}0701_0630_daily_all_RCC.csv'
    portfolio_to_read = os.path.join(portfoliopath, CSV_FILE_PATH)
    pdata = pd.read_csv(portfolio_to_read)
    pdata['date']=pd.to_datetime(pdata['date'])
    # Create datatime ticker from df_bar
    df_bar=pd.DataFrame()
    df_bar[['date','time']] = pd.date_range('09:30', '16:00', freq= '5min').to_series().apply(
                lambda x: pd.Series([i for i in str(x).split(" ")]))
    df_bar.index = range(len(df_bar))
    datecomplete = list(map(lambda x: x.strftime("%Y%m%d"),pdata.date))
    datelist=list(set(datecomplete))
    datelist.sort()
    pdata[['permno']]=pdata[['permno']].astype(int)
    
    outlieryear=pd.DataFrame()
    for k in range(0,7):
        index=[0, 35, 70, 105, 140, 175, 210, -1]
        # Controlling date range you want to include for intraday calculation
        pdstart = datelist[index[k]]
        pdend = datelist[index[k+1]]
        ind=(pdata['date'] >= pdstart) & (pdata['date']  <= pdend)
        DataPc=pdata[ind]

        DataPcnew=DataPc.copy()
        DataPcnew=DataPcnew[DataPcnew.prc>0]
        DataPcnew=DataPcnew.rename(columns={'prc':'prca'})

        DataPcnew['openprc']=DataPcnew['openprc'].abs()
        DataPcnew['INRet']=DataPcnew['prca']/DataPcnew['openprc']-1
        DataPcnew['ONRet']=(DataPcnew['retadj']+1)/(DataPcnew['INRet']+1)-1

        ###################
        # Step Two #
        ###################
        DataPcnewcopy=DataPcnew
        DataPcnewcopy=DataPcnewcopy.reset_index(drop=True)
        m = DataPcnew.reindex(np.repeat(DataPcnew.index.values, 79), method='ffill')
        m['Unnamed: 0'] = m.groupby(['date','permno']).cumcount()
        m=m.rename(columns={'Unnamed: 0':'intratime'})
        m['intratime']=np.where(m['intratime']<=78,m['intratime'],m['intratime']-79)
        m.index = range(len(m))
        m['prcadaily'] = m['prca']

        kd_m=m
        all_kd=pd.DataFrame()
        if index[k+1]+1!=0:
            for eachday in datelist[index[k]:index[k+1]+1]:
                matchtable_to_save = os.path.join(matchoutpath, "matchtable_"+str(eachday)+".csv")
                kd = pd.read_csv(matchtable_to_save)
                all_kd = pd.concat([all_kd,kd])
            all_kd['date']=pd.to_datetime(all_kd['date'])       
            kd_m=pd.merge(kd_m,all_kd,how='left',on=['permno','date'])

            all_p5m=pd.DataFrame()
            kd_m_p5m=kd_m.sort_values(by=['date','symbol','intratime'])
            for eachday in datelist[index[k]:index[k+1]+1]:
                taq_to_save = os.path.join(taqoutpath, "taq_"+str(eachday)+".csv")
                dff=pd.read_csv(taq_to_save)
                all_p5m = pd.concat([all_p5m,dff])   
            all_p5m['date']=pd.to_datetime(all_p5m['date'])    
            kd_m_p5m=pd.merge(kd_m_p5m, all_p5m, how='left',on=['date','symbol','intratime'])
            kd_m_p5m.reset_index(drop='True',inplace=True)
            kd_m_p5m['prca']=np.where((kd_m_p5m['intratime']+1)%79==0,kd_m_p5m['prcadaily'],kd_m_p5m['tprice'])

        else: 
            for eachday in datelist[index[k]:]:
                matchtable_to_save = os.path.join(matchoutpath, "matchtable_"+str(eachday)+".csv")
                kd = pd.read_csv(matchtable_to_save)
                all_kd = pd.concat([all_kd,kd])
            all_kd['date']=pd.to_datetime(all_kd['date'])       
            kd_m=pd.merge(kd_m,all_kd,how='left',on=['permno','date'])

            all_p5m=pd.DataFrame()
            kd_m_p5m=kd_m.sort_values(by=['date','symbol','intratime'])
            for eachday in datelist[index[k]:]:
                taq_to_save = os.path.join(taqoutpath, "taq_"+str(eachday)+".csv")
                dff=pd.read_csv(taq_to_save)
                all_p5m = pd.concat([all_p5m,dff])   
            all_p5m['date']=pd.to_datetime(all_p5m['date'])    
            kd_m_p5m=pd.merge(kd_m_p5m, all_p5m, how='left',on=['date','symbol','intratime'])
            kd_m_p5m.reset_index(drop='True',inplace=True)
            kd_m_p5m['prca']=np.where((kd_m_p5m['intratime']+1)%79==0,kd_m_p5m['prcadaily'],kd_m_p5m['tprice'])


        ###################
        # Step Three #
        ###################
        #ret_kd_m_p5m=kd_m_p5m.copy()
        ret_kd_m_p5m=kd_m_p5m
        #Clean nomatch
        ret_kd_m_p5m=ret_kd_m_p5m.dropna(axis=0,subset=['ticker'])
        ret=pd.DataFrame(index=ret_kd_m_p5m.index)
        ret['ret']=ret_kd_m_p5m['prca']/ret_kd_m_p5m.groupby(['symbol','date'])['prca'].shift(1)-1
        ret['ret']=np.where((ret_kd_m_p5m.index)%79==0,ret_kd_m_p5m['ONRet'],ret['ret'])
        #Control for price bounces
        ret['refmax']=ret_kd_m_p5m[['prcadaily','openprc']].max(axis=1)
        ret['refmin']=ret_kd_m_p5m[['prcadaily','openprc']].min(axis=1)
        coef=2
        condition1=(ret_kd_m_p5m['intratime']!=0)&(ret['refmin']>4)&((ret_kd_m_p5m['prca']>ret['refmax']*coef)|(ret_kd_m_p5m['prca']<ret['refmin']/coef))
        condition2=(ret_kd_m_p5m['intratime']!=0)&(ret['refmin']>4)&((ret_kd_m_p5m.groupby(['symbol','date'])['prca'].shift(1)>ret['refmax']*coef)|(ret_kd_m_p5m.groupby(['symbol','date'])['prca'].shift(1)<ret['refmin']/coef))&((ret_kd_m_p5m['prca']<=ret['refmax']*coef)&(ret_kd_m_p5m['prca']>=ret['refmin']/coef))
        ret_kd_m_p5m['retadj']=np.where(condition1, 0, ret['ret'])
        ret_kd_m_p5m['retadj']=np.where(condition2, (ret_kd_m_p5m['prca']/ret_kd_m_p5m['openprc'])-1, ret_kd_m_p5m['retadj'])
        ret_kd_m_p5m['retadj']=np.where((ret_kd_m_p5m.symbol=='BRKA')&(ret_kd_m_p5m.retadj.abs()>=0.3),0,ret_kd_m_p5m['retadj'])
        ret['1+ret']=1+ret_kd_m_p5m['retadj']
        ret['permno']=ret_kd_m_p5m['permno']
        ret['date']=ret_kd_m_p5m['date']
        ret['cumret']=ret.groupby(['permno','date'])['1+ret'].cumprod()
        ret['lcumret']=ret.groupby(['permno','date'])['cumret'].shift(1).fillna(1)
        ret_kd_m_p5m['wt'] = ret['lcumret']*ret_kd_m_p5m['wt']
        ret_kd_m_p5m['raw_ret']=ret['ret']
        outlier=pd.DataFrame()
        outlier=ret_kd_m_p5m[((ret_kd_m_p5m.symbol=='BRKA')&(ret_kd_m_p5m.retadj.abs()>=0.3))| condition1 | condition2]


        ###################
        # Step Four #
        ###################
        #mstep4=ret_kd_m_p5m.copy()
        mstep4=ret_kd_m_p5m

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

        ff_factors_bm['WB']=(ff_factors_bm['BL']+ff_factors_bm['BM']+ff_factors_bm['BH'])/3
        ff_factors_bm['WS']=(ff_factors_bm['SL']+ff_factors_bm['SM']+ff_factors_bm['SH'])/3
        ff_factors_bm['SMB_HML'] = ff_factors_bm['WS']-ff_factors_bm['WB']

        ff_factors_bm=ff_factors_bm[['date','intratime','HML','SMB_HML']]

        # Profit factor #
        mstep4_op=mstep4[mstep4['nonmissport_rw']==1]
        vwret_op=mstep4_op.groupby(['date','intratime','szport','rwport']).apply(wavg, 'retadj','wt').to_frame().reset_index().rename(columns={0: 'vwret'})
        vwret_op['srport']=vwret_op['szport']+vwret_op['rwport']
        ff_factors_op=vwret_op.pivot_table(index=['date','intratime'], columns='srport', values='vwret').reset_index()
        ff_factors_op['RMWR']=(ff_factors_op['BR']+ff_factors_op['SR'])/2
        ff_factors_op['RMWW']=(ff_factors_op['BW']+ff_factors_op['SW'])/2
        ff_factors_op['RMW'] = ff_factors_op['RMWR']-ff_factors_op['RMWW']

        ff_factors_op['WB']=(ff_factors_op['BW']+ff_factors_op['BM']+ff_factors_op['BR'])/3
        ff_factors_op['WS']=(ff_factors_op['SW']+ff_factors_op['SM']+ff_factors_op['SR'])/3
        ff_factors_op['SMB_RMW'] = ff_factors_op['WS']-ff_factors_op['WB']

        ff_factors_op=ff_factors_op[['date','intratime','RMW','SMB_RMW']]

        # Investment factor #
        mstep4_ca=mstep4[mstep4['nonmissport_ca']==1]
        vwret_ca=mstep4_ca.groupby(['date','intratime','szport','caport']).apply(wavg, 'retadj','wt').to_frame().reset_index().rename(columns={0: 'vwret'})
        vwret_ca['scport']=vwret_ca['szport']+vwret_ca['caport']
        ff_factors_ca=vwret_ca.pivot_table(index=['date','intratime'], columns='scport', values='vwret').reset_index()
        ff_factors_ca['CMAC']=(ff_factors_ca['BC']+ff_factors_ca['SC'])/2
        ff_factors_ca['CMAA']=(ff_factors_ca['BA']+ff_factors_ca['SA'])/2
        ff_factors_ca['CMA'] = ff_factors_ca['CMAC']-ff_factors_ca['CMAA']

        ff_factors_ca['WB']=(ff_factors_ca['BA']+ff_factors_ca['BM']+ff_factors_ca['BC'])/3
        ff_factors_ca['WS']=(ff_factors_ca['SA']+ff_factors_ca['SM']+ff_factors_ca['SC'])/3
        ff_factors_ca['SMB_CMA'] = ff_factors_ca['WS']-ff_factors_ca['WB']

        ff_factors_ca=ff_factors_ca[['date','intratime','CMA','SMB_CMA']]

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

        # Size factor #

        ff_factors_merged['SMB'] = (ff_factors_merged['SMB_HML']+ff_factors_merged['SMB_RMW']+ff_factors_merged['SMB_CMA'])/3
        ff_factors_merged.drop(columns=['SMB_HML','SMB_RMW','SMB_CMA'], inplace=True)

        ###################
        #  Saving results  #
        ###################
        outlieryear=pd.concat([outlieryear,outlier])
        out_to_save = os.path.join(outpath, pdstart+'_'+pdend[4:]+"_intraday.csv")
        eachoutlier_to_save = os.path.join(outlierpath, "outlier_"+pdstart+'_'+pdend[4:]+".csv")
        ff_factors_merged.to_csv(out_to_save)
        outlier.to_csv(eachoutlier_to_save, index=False)
    outliertotal=pd.concat([outliertotal,outlieryear])
outlier_to_save = os.path.join(outlierpath, "outlier_"+str(calstart)+'_'+str(calend)+".csv")
outliertotal.to_csv(outlier_to_save, index=False)

        