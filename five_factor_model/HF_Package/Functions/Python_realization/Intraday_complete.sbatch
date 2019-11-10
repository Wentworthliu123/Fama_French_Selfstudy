#!/bin/bash

#SBATCH --job-name=FF_All_factor
#SBATCH --ntasks=1
#SBATCH --mem-per-cpu=30g

module load python/3.7.0
source /project2/rcc/yuxing/dachxiu/data_usstock_taq/init.sh
python  Intraday_complete.py
