from pyspark import SparkContext
sc = SparkContext()

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)

tbl_Measure_Dates = sqlContext.sql("Select Measure_ID, Measure_Name, Measure_Start_Date, Measure_End_Date From tbl_Measure_Dates_RAW").rdd

tbl_Measure_Dates.saveAsTextFile("/user/w205/hospital_compare_txt_TRANSFORMED/txt_measure_dates/")
sqlContext.createDataFrame(tbl_Measure_Dates).write.parquet("/user/w205/hospital_compare_tbl_TRANSFORMED/tbl_measure_dates/")


exit()
