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

#transform all the tables in pyspark and save them as text files
/data/spark15/bin/pyspark /home/w205/Ex_1_github/transforming/transform_tbl_Hospitals.py
/data/spark15/bin/pyspark /home/w205/Ex_1_github/transforming/transform_tbl_Effective_Care.py
/data/spark15/bin/pyspark /home/w205/Ex_1_github/transforming/transform_tbl_Readmission.py
/data/spark15/bin/pyspark /home/w205/Ex_1_github/transforming/transform_tbl_Survey_Responses.py
/data/spark15/bin/pyspark /home/w205/Ex_1_github/transforming/transform_tbl_Measure_Dates.py



/data/spark15/bin/pyspark /home/w205/Ex_1_github/investigations/best_hospitals/best_hospitals.py
/data/spark15/bin/pyspark /home/w205/Ex_1_github/investigations/best_states/best_states.py


hdfs dfs -cat /user/w205/hospital_compare_INVESTIGATIONS/best_hospitals/*
hdfs dfs -cat /user/w205/hospital_compare_INVESTIGATIONS/best_states/*