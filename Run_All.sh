#Start Hive Metastore
/data/start_metastore.sh

#download data and store in HDFS
bash /home/w205/Ex_1_github/load_data_lake.sh

#Create Tables
hive -f /home/w205/Ex_1_github/hive_base_ddl.sql

#Start pyspark
/data/spark15/bin/pyspark
