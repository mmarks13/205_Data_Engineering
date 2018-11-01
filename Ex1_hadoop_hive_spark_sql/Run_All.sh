#Start Hive Metastore
/data/start_metastore.sh

#make sure pandas is installed
pip install pandas

#Set pyspark environment variable
export PYSPARK_PYTHON=/usr/bin/python

#download data and store in HDFS
bash /home/w205/Ex_1_github/loading_and_modelling/load_data_lake.sh

#Create Tables
hive -f /home/w205/Ex_1_github/loading_and_modelling/hive_base_ddl.sql

#transform all the tables in pyspark and save them as text files
/data/spark15/bin/spark-submit /home/w205/Ex_1_github/transforming/transform_tbl_Hospitals.py
/data/spark15/bin/spark-submit /home/w205/Ex_1_github/transforming/transform_tbl_Effective_Care.py
/data/spark15/bin/spark-submit /home/w205/Ex_1_github/transforming/transform_tbl_Readmission.py
/data/spark15/bin/spark-submit /home/w205/Ex_1_github/transforming/transform_tbl_Survey_Responses.py
/data/spark15/bin/spark-submit /home/w205/Ex_1_github/transforming/transform_tbl_Measure_Dates.py

/data/spark15/bin/spark-submit /home/w205/Ex_1_github/investigations/best_hospitals/best_hospitals.py
/data/spark15/bin/spark-submit /home/w205/Ex_1_github/investigations/best_states/best_states.py
/data/spark15/bin/spark-submit /home/w205/Ex_1_github/investigations/hospital_variability/hospital_variability.py
/data/spark15/bin/spark-submit /home/w205/Ex_1_github/investigations/hospitals_and_patients/hospitals_and_patients.py

echo "Top Ten Hospitals for Chosen Quality Measures"
hdfs dfs -cat /user/w205/hospital_compare_INVESTIGATIONS/best_hospitals/*

echo "Top Ten States for Chosen Quality Measures"
hdfs dfs -cat /user/w205/hospital_compare_INVESTIGATIONS/best_states/*

echo "Top Ten Most Variable Measures Between Hospitals"
hdfs dfs -cat /user/w205/hospital_compare_INVESTIGATIONS/hospital_variability/*

