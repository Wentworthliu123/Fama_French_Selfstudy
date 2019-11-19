## Information collection
- TAQ CRSP Link
    WRDS provides direct querying method for the matching table of CRSP and TAQ. The entire table is of 190mb if using csv format.
    https://wrds-web.wharton.upenn.edu/wrds/ds/wrdsapps/link/taqcrsplink/index.cfm?navId=539

- New CTLINK SAS macro 
    The WRDS macro was last updated in 2010 and NYSE has since changed the TAQ specifications, so the macro no longer works properly. This new version, named TCLINK_v3.sas, works on the millisecond version of the TAQ Master files, which have consistent data beginning in 2011. The old TCLINK macro will work prior to 2011 with no changes.

    The TCLINK_v3 macro returns trading symbol root-symbol suffix-CRSP PERMNO pairings and the continuous date ranges that apply. This is different than the output of the WRDS macro, which had pairings given for each date.

    To use the macro include it in your program like usual (e.g., %include 'TCLINK_v3.sas). You must have subscriptions to the TAQ product and the CRSP product on WRDS, and the code must be run on the WRDS Cloud Server.
    https://github.com/tbeason/TAQ-CRSP-Link


## SAS program testing
- Able to run SAS sample code correctly
    This link is the SAS TCLINK sample from wrds:
    https://wrds-www.wharton.upenn.edu/pages/support/research-wrds/macros/wrds-macro-tclink/
    This is the SAS code location on server:
    - Within the time range, matching list of each date will be concated to a single long one
    - Matching frequency is monthly, and for dates after 2015 this program will not run properly
    - Output can be generated in the same SAS program
    /home/uchicago/dachxiu/xinyu/SAS_fama_french/Permno_ticker/TCLINK_WRDS.sas

- Able to run SAS upgraded code from online correctly
    - One permno may be associated with more than one symbols, much match with the one that's traded on US market through cusip

## TAQ name information locations
- Monthly old fashion(Available from 199301 to 201412)
    eg: /wrds/taq/sasdata/MAST_200904
- Daily current fashion(Available from 20090225)
    eg: /wrds/nyse/sasdata/taqms/mast/
- TAQ.NAMEs(Potentially needed, not sure)
    eg: /wrds/taq/sasdata/NAMES





