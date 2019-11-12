## Goals
- To convert the matlab intraday factor calculation project into python
>The previous RA separate the procedure into 4 steps
>/project2/dachxiu/hf_ff_project/Implmt_Code/Xinyu_test/

## Data sources
- For the matlab project there are already formatted inputs that contains crsp returns, TAQ intraday returns and factors, I must either create the factor put file the same way structured in previous project, or adjust the matlab code workflow
- There are two existing location to extract CRSP data and daily index, an additional source provides API to access TAQ data
> source /project2/rcc/yuxing/dachxiu/data_usstock_taq/init.sh
> module load python/3.7.0
> /sbin/ip route get 8.8.8.8 | awk '{print $NF;exit}'
> 
Python soft link:
> ln -s /project2/rcc/yuxing/dachxiu/project_qmle/usstock/data_api/python/stock.so stock.so
Matlab:
> /project2/rcc/yuxing/dachxiu/data_usstock_taq/api/mex

### Step one 1102
dataname = [alldataPath,'alldata_',sd,ed,'_fixed.mat']
This line above loads data from the path below(which is different from the pathfile setting)
/project2/dachxiu/hf_ff_project/CRSP_data

### Step two 1103
path = '/project2/dachxiu/mye/adj_index_day/index_day/';
file = [readtime,'.txt'];
This is the second part of data, which I assume is derived from TAQ
Also to employ the function stock5min, one has to set up the environment by adding necessary files to the working directory:
>/project2/rcc/yuxing/dachxiu/project_qmle/usstock/data_api/mex/Stock5Min.mexa64

## Words from professor Xiu
so every year, we need to run one version of your code to do portfolio sorting
and then every day, we need to find CRSP data, and give the portfolio return.
In other words, I want you to have in mind this
We need to decompose your code into two.
Alright, then now you are working on the third part, filling in the HF data.
So all you need to do is to fill in high frequency 5-min returns that Yuxing created, in between CRSP prices.
and obtain new returns.
The tricky part is that the close prices are unadjusted for dividend and splits from high frequency data.
So we will have to use CRSP close unajdusted price and open price, to back out the right overnight return.
and then attach that with 78 5-minute high frequcy returns, the job is done.
So it is hypothetically very easy.
The tricky part is 1. calculating overnight return, 2. matching CRSP with TAQ.
All these are done in matlab. We need to redo this in python, and do it in a way that the program do not need to run for the whole history.

So the flow should be: 1. sorting portfolio if today is the end of month or year. 2. form daily portfolio return for today. 3. fill in high frequency return, and obtain the vector of 1+78 = 79 number of returns.

So the codes are in /project2/dachxiu/hf_ff_project

also take a look at the hf_fama_french.
i know the Hausmanxxx inside the latter folder is not about this.
I think the former folder is the right one. The previous RA separate the procedure into 4 steps.
So my suggestion is you do it for one day.
then we can let the program run automatically, to get everything else.
Of course, to test your code, you can try running for a few months.
In github, there should be instructions on how to link to the python database.
Also, in the beginning, try not to pay too much attention on the ticker mismatch problem between TAQ and CRSP. We will leave that to the end. (edited) 
Also, to make sure that you can get good result, you can compare your high frequency returns with matlab codes.
We tried to use CRSP close prices instead of TAQ, because the former is more reliable.
Alright, you can dig into this, and let me know if you have questions.

## Meeting recap with Yuxing regarding checking ticker matching
To do next(20191108):
- Access the checking program
- Produce historical txt file 
- Compare with cleaned version on RCC
Also:
- Need to fix the RCC running issue

## Meeting with Yuxing on matching symbol and data downloading
20191111
1. Pertaining to matching tickers, based on previous RAs work, Yuxing has created a program that can generate symbol-ticker map every day (quarterly updated). Basically starting from 2007, TAQ began to distinguish symbols using "/", so the mapping issue seemed not to be an issue. He also made new APIs for me to easily query it in python. I'll first replace the historical txt file source with this API, and then double check whether this mapping provides same result.

2. Regarding CRSP data connection, I helped Yuxing identify all CRSP, COMPUSTAT sub library that he needs to download to our datebase in order to realize intraday factor self-update, including CRSP monthly, delist, eventnames, linktable, compustat, etc. We discussed what would be the good way to design the API and data structure, at the same time minimize changes to exisitng code. He is now working on completing the database and making corresponding APIs.

3. In the next step, we will keep refining the code and adjust it to a proper form suitable for self-update.


## Information useful to do conduct this project:

### Running Jupyter notebook on RCC
https://rcc.uchicago.edu/docs/software/environments/python/index.html#mdoc-python
>/sbin/ip route get 8.8.8.8 | awk '{print $NF;exit}'
>jupyter-notebook --no-browser --ip=128.135.112.69
This change every time  




### Matching CRSP and TAQ Data
https://wrds-www.wharton.upenn.edu/pages/support/support-articles/taq/matching-crsp-and-taq-data/?_ga=2.167687094.90502454.1572579381-1924758175.1568907974
Matching TAQ with CRSP
z Match symbol to corresponding master table to retrieve TAQ CUSIP
z Use first 8 characters of TAQ’s CUSIP to match with CRSP
NCUSIP (dsfnames or msfnames) and retrieve the PERMNO
z If you need Compustat data, use the merged CRSPCompustat files (using the NPERMNO from the cstlink file)

### TAQ CRSP Link provides the link between TAQ SYMBOL and CRSP PERMNO
https://wrds-web.wharton.upenn.edu/wrds/query_forms/navigation.cfm?navId=539

### WRDS Macro: TCLINK: Create TAQ-CRSP Link Table
https://wrds-www.wharton.upenn.edu/pages/support/research-wrds/macros/wrds-macro-tclink/


### TAQ User’s Guide
https://wrds-www.wharton.upenn.edu/documents/766/TAQ_Users_Guide_11-2012_edition.pdf?_ga=2.100094870.90502454.1572579381-1924758175.1568907974

### Matlab to Python tools
https://github.com/awesomebytes/libermate
https://github.com/victorlei/smop

### Introducing SASPy: Use Python code to access SAS
https://blogs.sas.com/content/sasdummy/2017/04/08/python-to-sas-saspy/

