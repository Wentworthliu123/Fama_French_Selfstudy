import os
import re
year='2011'
oldyear='2012'
evenold=str(int(oldyear)+1)
# replacement strings
WINDOWS_LINE_ENDING = b'\r\n'
UNIX_LINE_ENDING = b'\n'
filepath = "F:\\RA_Fama_French_Factor\\five_factor_model\\HF_Package\\Comparison_PY_MAT_FF\\Calculation by year"
if __name__ == "__main__":
    print("start")
    if not os.path.exists(filepath):
        print("目录不存在!!")
        os._exit(1)
    filenames = os.listdir(filepath)
    for data in filenames:
        if data[:4]==oldyear:
            newname = year + data[4:]
        else:
            newname = year + data
        print(newname)
        os.rename(filepath + '\\' + data,filepath + '\\' + newname)
        
    filenames = os.listdir(filepath)
    new_filelist=[col for col in filenames if "Intraday_RCC_newmatching" in col]
    for data in new_filelist:
        lis=data.split('.')
        if lis[1]=='sbatch':
            f = open(data, "r+")
            s=f.read()#读出 
            f.seek(0,0) #指针移到头  原来的数据还在 是替换 会存在一个问题 如果少   会替换不了全部数据，自已思考解决!!!
            #从头写入
            f.write(s.replace(oldyear+"Intraday_RCC_newmatching_",year+"Intraday_RCC_newmatching_"))
            f.write(s.replace(f"start={oldyear}0701",f"start={oldyear}0701"))
            f.write(s.replace(f"end={evenold}0630",f"end={oldyear}0630"))

            f.close()
            
            with open(data, 'rb') as open_file:
                content = open_file.read()
            content = content.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)

            with open(data, 'wb') as open_file:
                open_file.write(content)
            

        elif lis[1]=='py':
            f = open(data, "r+")
            s=f.read()#读出 
            f.seek(0,0) 
            #从头写入
            f.write(s.replace(f"CSV_FILE_PATH = '{oldyear}0701_0630_daily_all_RCC.csv'",f"CSV_FILE_PATH = '{year}0701_0630_daily_all_RCC.csv'"))
            f.close()
            

            