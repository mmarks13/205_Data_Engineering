from pyspark import SparkContext
sc = SparkContext()

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)

tbl_Hospitals = sqlContext.sql("select * from tbl_Hospitals_RAW").rdd

tbl_Hospitals.saveAsTextFile("/user/w205/hospital_compare_TRANSFORMED/tbl_hospitals/")

exit()
