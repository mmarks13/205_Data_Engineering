#Start pyspark  
/data/spark15/bin/pyspark

tbl_Measure_Dates = sqlContext.sql("Select Measure_ID, Measure_Name, Measure_Start_Quarter, Measure_Start_Date, Measure_End_Quarter, Measure_End_Date From tbl_Measure_Dates_RAW").rdd

tbl_Measure_Dates.saveAsTextFile("/user/w205/hospital_compare_TRANSFORMED/tbl_measure_dates/")

exit()
