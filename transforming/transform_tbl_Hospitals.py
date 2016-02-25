from pyspark import SparkContext
sc = SparkContext()

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)

tbl_Hospitals = sqlContext.sql("select * from tbl_Hospitals_RAW").rdd

tbl_Hospitals.saveAsTextFile("/user/w205/hospital_compare_txt_TRANSFORMED/txt_hospitals/")
sqlContext.createDataFrame(tbl_Hospitals).write.parquet("/user/w205/hospital_compare_tbl_TRANSFORMED/tbl_hospitals/")


exit()
