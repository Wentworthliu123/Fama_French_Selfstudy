import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from pandas.core.frame import DataFrame
from scipy import stats
import datetime
import DataHub as hub

def portfolio(start,end):
    ######################
    # Time Range Setting #
    ######################
    #daterange for portfolio construction
    startdate = 20120101
    enddate = 20171231

    #daterange for intraday
    rangestart = start
    rangeend = end
    ###################
    # Compustat Block #
    ###################
    Compustat = hub.Handle.create('Compustat')
    comp = Compustat.read('AnnualFundamental', start=startdate, end=enddate)

    #convert datadate to date fmt
    comp['datadate']=pd.to_datetime(comp['datadate'].astype(str), format='%Y%m%d')
    comp['year']=comp['datadate'].dt.year
    #eg:from 2015-02-04锛坉type: object锛?to 2015-02-04(datetime64[ns])
    #create a new column for 'year'


    # create preferrerd stock
    comp['ps']=np.where(comp['pstkrv'].isnull(), comp['pstkl'], comp['pstkrv'])
    comp['ps']=np.where(comp['ps'].isnull(),comp['pstk'], comp['ps'])
    comp['ps']=np.where(comp['ps'].isnull(),0,comp['ps'])
    #manipulate ps data in the sequense of redemption, liquidating and total value, last resolution is 0

    comp['txditc']=comp['txditc'].fillna(0)

    # create book equity
    comp['be']=comp['seq']+comp['txditc']-comp['ps']
    comp['be']=np.where(comp['be']>0, comp['be'], np.nan)
    # create operating profit
    comp['revt']=comp['revt'].fillna(0)
    comp['cogs']=comp['cogs'].fillna(0)
    comp['xsga']=comp['xsga'].fillna(0)
    comp['xint']=comp['xint'].fillna(0)
    comp['op']=np.where(comp['be']>0, (comp['revt']-comp['cogs']-comp['xsga']-comp['xint'])/comp['be'], np.nan)
    # create investment
    comp['lat']=comp.sort_values(by=['datadate'], ascending=True).groupby(['gvkey'])['at'].shift(1)
    comp['inv']=(comp['lat']-comp['at'])/comp['at']


    # number of years in Compustat
    comp=comp.sort_values(by=['gvkey','datadate'])
    comp['count']=comp.groupby(['gvkey']).cumcount()
    #Sort DataFrame by column gvkey and datadate
    #Mark cumulative number of each gvkey as of that row, starting from 0

    comp=comp[['gvkey','datadate','year','be','op','inv','count']]

    ###################
    # CRSP Block      #
    ###################
    CRSP = hub.Handle.create('CRSP')
    crsp_m = CRSP.read('MonthlyStock', start=startdate, end=enddate)

    # change variable format to int
    crsp_m[['permco','permno','shrcd','exchcd']]=crsp_m[['permco','permno','shrcd','exchcd']].astype(int)

    # Line up date to be end of month
    crsp_m['date']=pd.to_datetime(crsp_m['date'].astype(str), format='%Y%m%d')
    crsp_m['jdate']=crsp_m['date']+MonthEnd(0)
    #The 1 in MonthEnd just specifies to move one step forward to the next date that's a month end.

    # add delisting return
    dlret = CRSP.read('DelistHistory', start=startdate, end=enddate)

    #MSEDELIST		CRSP Monthly Stock Event - Delisting
    #DLRET 	Num	8	Delisting Return,DLRET is the return of the security after it is delisted. 
    #It is calculated by comparing a value after delisting against the price on the security's last trading date. 
    #The value after delisting can include a delisting price or the amount from a final distribution.
    #DLSTDT 	Num	8	Delisting Date,DLSTDT contains the date (in YYMMDD format) of a security's last price on the current exchange.

    #process dlret
    dlret.permno=dlret.permno.astype(int)
    dlret['dlstdt']=pd.to_datetime(dlret['dlstdt'].astype(str), format='%Y%m%d')
    dlret['jdate']=dlret['dlstdt']+MonthEnd(0)

    #merge dlret and crsp_m
    crsp = pd.merge(crsp_m, dlret, how='left',on=['permno','jdate'])
    #crsp and dlret share the same column names: permno and jdate

    #process crsp
    crsp['dlret']=crsp['dlret'].fillna(0)
    crsp['ret']=crsp['ret'].fillna(0)
    crsp['retadj']=(1+crsp['ret'])*(1+crsp['dlret'])-1

    # calculate market equity
    crsp['me']=crsp['prc'].abs()*crsp['shrout'] 
    #market equity equals to price of stock times shares of outstanding

    #process crsp
    crsp=crsp.drop(['dlret','dlstdt','prc','shrout'], axis=1)
    crsp=crsp.sort_values(by=['jdate','permco','me'])

    ### Aggregate Market Cap ###
    # sum of me across different permno belonging to same permco a given date
    crsp_summe = crsp.groupby(['jdate','permco'])['me'].sum().reset_index()
    # largest mktcap within a permco/date
    crsp_maxme = crsp.groupby(['jdate','permco'])['me'].max().reset_index()
    # join by jdate/maxme to find the permno
    crsp1=pd.merge(crsp, crsp_maxme, how='inner', on=['jdate','permco','me'])
    # drop me column and replace with the sum me
    crsp1=crsp1.drop(['me'], axis=1)
    # join with sum of me to get the correct market cap info
    crsp2=pd.merge(crsp1, crsp_summe, how='inner', on=['jdate','permco'])
    # sort by permno and date and also drop duplicates
    crsp2=crsp2.sort_values(by=['permno','jdate']).drop_duplicates()
    # important to have a duplicate check

    # keep December market cap
    crsp2['year']=crsp2['jdate'].dt.year
    crsp2['month']=crsp2['jdate'].dt.month
    decme=crsp2[crsp2['month']==12]
    decme=decme[['permno','date','jdate','me','year']].rename(columns={'me':'dec_me'})

    ### July to June dates
    crsp2['ffdate']=crsp2['jdate']+MonthEnd(-6)
    crsp2['ffyear']=crsp2['ffdate'].dt.year
    crsp2['ffmonth']=crsp2['ffdate'].dt.month
    crsp2['1+retx']=1+crsp2['retx']
    crsp2=crsp2.sort_values(by=['permno','date'])

    # cumret by stock
    crsp2['cumretx']=crsp2.groupby(['permno','ffyear'])['1+retx'].cumprod()
    #cumprod returns the product of the year in this case, which is the cumulative return as time goes by

    # lag cumret
    crsp2['lcumretx']=crsp2.groupby(['permno'])['cumretx'].shift(1)

    # lag market cap
    crsp2['lme']=crsp2.groupby(['permno'])['me'].shift(1)

    # if first permno then use me/(1+retx) to replace the missing value
    crsp2['count']=crsp2.groupby(['permno']).cumcount()
    crsp2['lme']=np.where(crsp2['count']==0, crsp2['me']/crsp2['1+retx'], crsp2['lme'])

    # baseline me
    mebase=crsp2[crsp2['ffmonth']==1][['permno','ffyear', 'lme']].rename(columns={'lme':'mebase'})

    # merge result back together
    crsp3=pd.merge(crsp2, mebase, how='left', on=['permno','ffyear'])
    crsp3['wt']=np.where(crsp3['ffmonth']==1, crsp3['lme'], crsp3['mebase']*crsp3['lcumretx'])

    decme['year']=decme['year']+1
    decme=decme[['permno','year','dec_me']]

    # Info as of June
    crsp3_jun = crsp3[crsp3['month']==6]

    crsp_jun = pd.merge(crsp3_jun, decme, how='inner', on=['permno','year'])
    crsp_jun=crsp_jun[['permno','date', 'jdate', 'shrcd','exchcd','retadj','me','wt','cumretx','mebase','lme','dec_me']]
    crsp_jun=crsp_jun.sort_values(by=['permno','jdate']).drop_duplicates()

    #######################
    # CCM Block           #
    #######################
    ccm = CRSP.read('CompustatLink', start=10000000, end=29999999)
    ccm.rename(columns={'lpermno':'permno'},inplace=True)
    #CCMXPF_LINKTABLE		CRSP/COMPUSTAT Merged - Link History w/ Used Flag
    #lpermno 	Num	8	Historical CRSP PERMNO Link to COMPUSTAT Record
    # linktype 	Char	2	Link Type Code,
    # Link Type Code is a 2-character code providing additional detail on the usage of the link data available.
    # linkprim 	Char	1	Primary Link Marker
    # linkdt 	Num	8	First Effective Date of Link
    # linkenddt 	Num	8	Last Effective Date of Link

    ccm['linkdt']=pd.to_datetime(ccm['linkdt'].astype(str), format='%Y%m%d')
    ccm['linkenddt']=pd.to_datetime(ccm['linkenddt'].astype(str), format='%Y%m%d')
    # if linkenddt is missing then set to today date
    ccm['linkenddt']=ccm['linkenddt'].fillna(pd.to_datetime('today'))
    #attention: pd.to.datetime does not convert today(M8[ns]) into format '%Y\%m\%d', need to go with ccm[].dt.date
    # if using the code below there will be warning on server
    # eg: ccm['linkenddt']=ccm['linkenddt'].dt.date

    ccm1=pd.merge(comp[['gvkey','datadate','be','op','inv','count']],ccm,how='left',on=['gvkey'])
    ccm1['yearend']=ccm1['datadate']+YearEnd(0)
    ccm1['jdate']=ccm1['yearend']+MonthEnd(6)

    # set link date bounds
    ccm2=ccm1[(ccm1['jdate']>=ccm1['linkdt'])&(ccm1['jdate']<=ccm1['linkenddt'])]
    ccm2=ccm2[['gvkey','permno','datadate','yearend','jdate','be','op','inv','count']]

    # link comp and crsp
    ccm_jun=pd.merge(crsp_jun, ccm2, how='inner', on=['permno', 'jdate'])
    ccm_jun['beme']=ccm_jun['be']*1000/ccm_jun['dec_me']

    # select NYSE stocks for bucket breakdown
    # exchcd = 1 and positive beme and positive me and shrcd in (10,11) and at least 2 years in comp
    nyse=ccm_jun[(ccm_jun['exchcd']==1) & (ccm_jun['beme']>0) & (ccm_jun['me']>0) & (ccm_jun['count']>=1) & ((ccm_jun['shrcd']==10) | (ccm_jun['shrcd']==11))]

    #####
    # size breakdown
    nyse_sz=nyse.groupby(['jdate'])['me'].median().to_frame().reset_index().rename(columns={'me':'sizemedn'})
    # beme breakdown
    nyse_bm=nyse.groupby(['jdate'])['beme'].describe(percentiles=[0.3, 0.7]).reset_index()
    nyse_bm=nyse_bm[['jdate','30%','70%']].rename(columns={'30%':'bm30', '70%':'bm70'})
    # op breakdown
    nyse_op=nyse.groupby(['jdate'])['op'].describe(percentiles=[0.3, 0.7]).reset_index()
    nyse_op=nyse_op[['jdate','30%','70%']].rename(columns={'30%':'op30', '70%':'op70'})
    # inv breakdown
    nyse_inv=nyse.groupby(['jdate'])['inv'].describe(percentiles=[0.3, 0.7]).reset_index()
    nyse_inv=nyse_inv[['jdate','30%','70%']].rename(columns={'30%':'inv30', '70%':'inv70'})

    # join together factor breakdown
    nyse_breaks_bm = pd.merge(nyse_sz, nyse_bm, how='inner', on=['jdate'])
    nyse_breaks_op = pd.merge(nyse_breaks_bm, nyse_op, how='inner', on=['jdate'])
    nyse_breaks_inv = pd.merge(nyse_breaks_op, nyse_inv, how='inner', on=['jdate'])

    # join back factor breakdown
    ccm1_jun = pd.merge(ccm_jun, nyse_breaks_inv, how='left', on=['jdate'])


    # function to assign sz and bm bucket
    def sz_bucket(row):
        if row['me']==np.nan:
            value=''
        elif row['me']<=row['sizemedn']:
            value='S'
        else:
            value='B'
        return value

    def bm_bucket(row):
        if 0<=row['beme']<=row['bm30']:
            value = 'L'
        elif row['beme']<=row['bm70']:
            value='M'
        elif row['beme']>row['bm70']:
            value='H'
        else:
            value=''
        return value

    def rw_bucket(row):
        if row['op']<=row['op30']:
            value = 'W'
        elif row['op']<=row['op70']:
            value='M'
        elif row['op']>row['op70']:
            value='R'
        else:
            value=''
        return value

    def ca_bucket(row):
        if row['inv']<=row['inv30']:
            value ='A'
        elif row['inv']<=row['inv70']:
            value='M'
        elif row['inv']>row['inv70']:
            value='C'
        else:
            value=''
        return value

    # assign size portfolio
    ccm1_jun['szport']=np.where((ccm1_jun['beme']>0)&(ccm1_jun['me']>0)&(ccm1_jun['count']>=1), ccm1_jun.apply(sz_bucket, axis=1), '')
    # assign book-to-market portfolio
    ccm1_jun['bmport']=np.where((ccm1_jun['beme']>0)&(ccm1_jun['me']>0)&(ccm1_jun['count']>=1), ccm1_jun.apply(bm_bucket, axis=1), '')
    # assign operating profit portfolio
    ccm1_jun['rwport']=np.where((ccm1_jun['beme']>0)&(ccm1_jun['me']>0)&(ccm1_jun['count']>=1), ccm1_jun.apply(rw_bucket, axis=1), '')
    # assign investment portfolio
    ccm1_jun['caport']=np.where((ccm1_jun['beme']>0)&(ccm1_jun['me']>0)&(ccm1_jun['count']>=1), ccm1_jun.apply(ca_bucket, axis=1), '')
    # create positivebmeme and nonmissport variable
    ccm1_jun['posbm']=np.where((ccm1_jun['beme']>0)&(ccm1_jun['me']>0)&(ccm1_jun['count']>=1), 1, 0)
    ccm1_jun['nonmissport_bm']=np.where((ccm1_jun['bmport']!=''), 1, 0)
    ccm1_jun['nonmissport_rw']=np.where((ccm1_jun['rwport']!=''), 1, 0)
    ccm1_jun['nonmissport_ca']=np.where((ccm1_jun['caport']!=''), 1, 0)
    # store portfolio assignment as of June
    june=ccm1_jun[['permno','date', 'jdate', 'bmport','rwport','szport','caport','posbm','nonmissport_bm','nonmissport_rw','nonmissport_ca']]
    june['ffyear']=june['jdate'].dt.year

    #######################################################
    # Create Momentum Portfolio                           #   
    # Measures Based on Past (J) Month Compounded Returns #
    #######################################################
    J = 11 

    ccm5=crsp3[(crsp3['wt']>0) & ((crsp3['shrcd']==10) | (crsp3['shrcd']==11))].copy()
    ccm5['ret']=ccm5['retadj']
    _tmp_crsp = ccm5[['permno','date','ret','me','exchcd']].sort_values(['permno','date']).set_index('date')
    # Replace missing return with 0
    _tmp_crsp['ret']=_tmp_crsp['ret'].fillna(0)
    # Calculate rolling cumulative return
    # by summing log(1+ret) over the formation period
    _tmp_crsp['logret']=np.log(1+_tmp_crsp['ret'])
    umdr = _tmp_crsp.groupby(['permno','exchcd'])['logret'].rolling(J, min_periods=J).sum()
    _tmp_crsp=_tmp_crsp.reset_index()
    _tmp_crsp=_tmp_crsp.drop(columns=['logret'])
    umdr = umdr.reset_index()
    umd = pd.merge(_tmp_crsp,umdr,how='left',on=['date','permno','exchcd'])
    umd['raw_cumret']=np.exp(umd['logret'])-1
    umd['cumret']=umd.groupby(['permno'])['raw_cumret'].shift(1)
    umd=umd.dropna(axis=0, subset=['cumret','me'])
    nysemom=umd[umd['exchcd']==1]

    nyse_mom=nysemom.groupby(['date'])['cumret'].describe(percentiles=[0.3, 0.7]).reset_index()
    nyse_mom=nyse_mom[['date','30%','70%']].rename(columns={'30%':'cumret30', '70%':'cumret70'})
    ccm1_mom = pd.merge(umd, nyse_mom, how='left', on=['date'])

    def mom_bucket(row):
        if row['cumret']<=row['cumret30']:
            value = 'L'
        elif row['cumret']<=row['cumret70']:
            value='M'
        elif row['cumret']>row['cumret70']:
            value='H'
        else:
            value=''
        return value

    ccm1_mom['momport']=ccm1_mom.apply(mom_bucket, axis=1)
    ccm1_mom['momposbm']=np.where((ccm1_mom['me']>0), 1, 0)
    ccm1_mom['nonmissport_mom']=np.where((ccm1_mom['momport']!=''), 1, 0)
    ccm1_mom['jdate']=ccm1_mom['date']+MonthEnd(0)+MonthEnd(1)
    everymom=ccm1_mom[['permno','date', 'jdate', 'momport','momposbm','nonmissport_mom']]

    ############################
    # Import daily data #
    ############################

    crsp_d = CRSP.read('DailyStock', fields ='permno,permco,date,openprc,prc,ret,retx,shrout', permno='%', start=rangestart, end=rangeend)

    # change variable format to int
    crsp_d[['permco','permno']]=crsp_d[['permco','permno']].astype(int)
    # Line up date to be end of month
    crsp_d['date']=pd.to_datetime(crsp_d['date'].astype(str), format='%Y%m%d')
    #####################
    crsp_d['jdate']=crsp_d['date']+MonthEnd(0)
    crsp_d['ffdate']=crsp_d['jdate']+MonthEnd(-6)
    crsp_d['ffyear']=crsp_d['ffdate'].dt.year
    #The 1 in MonthEnd just specifies to move one step forward to the next date that's a month end.

    # add delisting return
    dlret_d = CRSP.read('DelistHistory', start=rangestart, end=rangeend)

    #process dlret
    dlret_d.permno=dlret_d.permno.astype(int)
    dlret_d['dlstdt']=pd.to_datetime(dlret_d['dlstdt'].astype(str), format='%Y%m%d')
    #######################
    dlret_d['date']=dlret_d['dlstdt']

    #merge dlret and crsp_m
    crspd = pd.merge(crsp_d, dlret_d, how='left',on=['permno','date'])
    #crsp and dlret share the same column names: permno and jdate

    #process crsp
    crspd['dlret']=crspd['dlret'].fillna(0)
    crspd['ret']=crspd['ret'].fillna(0)
    crspd['retadj']=(1+crspd['ret'])*(1+crspd['dlret'])-1

    # calculate market equity
    crspd['me']=crspd['prc'].abs()*crspd['shrout'] 
    #market equity equals to price of stock times shares of outstanding

    #process crsp
    #crspd=crspd.drop(['dlret','dlstdt','prc','shrout'], axis=1)
    crspd=crspd.sort_values(by=['date','permco','me'])

    ### Aggregate Market Cap ###
    # sum of me across different permno belonging to same permco a given date
    crspd_summe = crspd.groupby(['date','permco'])['me'].sum().reset_index()
    # largest mktcap within a permco/date
    crspd_maxme = crspd.groupby(['date','permco'])['me'].max().reset_index()
    # join by jdate/maxme to find the permno
    crspd1=pd.merge(crspd, crspd_maxme, how='inner', on=['date','permco','me'])
    # drop me column and replace with the sum me
    crspd1=crspd1.drop(['me'], axis=1)
    # join with sum of me to get the correct market cap info
    crspd2=pd.merge(crspd1, crspd_summe, how='inner', on=['date','permco'])
    # sort by permno and date and also drop duplicates
    crspd2=crspd2.sort_values(by=['permno','date']).drop_duplicates()
    # important to have a duplicate check

    # lag market cap
    crspd2['lme']=crspd2.groupby(['permno'])['me'].shift(1)

    # if first permno then use me/(1+retx) to replace the missing value
    crspd2['count']=crspd2.groupby(['permno']).cumcount()
    crspd2['lme']=np.where(crspd2['count']==0, crspd2['me']/(1+crspd2['retx']), crspd2['lme'])
    # baseline me

    # merge result back together
    crspd2['wt']=crspd2['lme']

    # merge back with monthly records
    crspd2 = crspd2[['permno','retadj','me','wt','ffyear','jdate','date','dlret','dlstdt','openprc','prc','shrout']]
    crspd2mom = pd.merge(crspd2, everymom[['permno', 'jdate', 'momport','momposbm','nonmissport_mom']], how='left', on=['permno','jdate'])
    ccm3=pd.merge(crspd2mom, june[['permno','ffyear','bmport','rwport','szport','caport','posbm','nonmissport_bm','nonmissport_rw','nonmissport_ca']], how='left', on=['permno','ffyear'])

    # keeping only records that meet the criteria
    ccm4=ccm3[(ccm3['wt']>0) & (ccm3['posbm']==1) &  (ccm3['momposbm']==1)]
    ccm4=ccm4.drop_duplicates(subset=['date','permno'], keep='last').drop(['Unnamed: 0'],axis=1)
    ccm4.to_csv("{}_{}_daily_all_RCC.csv".format(str(rangestart),str(rangeend)[4:]))
    return ccm4