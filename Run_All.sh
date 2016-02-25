#Start Hive Metastore
/data/start_metastore.sh

#make sure pandas is installed
pip install pandas

#Set pyspark environment variable
export PYSPARK_PYTHON=/usr/bin/python

#download data and store in HDFS
bash /home/w205/Ex_1_github/load_data_lake.sh

#Create Tables
hive -f /home/w205/Ex_1_github/hive_base_ddl.sql


#transform all the tables and save them as text files
bash /home/w205/Ex_1_github/tranforming/transform_tbl_Hospitals.sh
bash /home/w205/Ex_1_github/tranforming/transform_tbl_Effective_Care.sh
bash /home/w205/Ex_1_github/tranforming/transform_tbl_Readmission.sh
bash /home/w205/Ex_1_github/tranforming/transform_tbl_Survey_Responses.sh
bash /home/w205/Ex_1_github/tranforming/transform_tbl_Measure_Dates.sh