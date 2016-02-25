from pyspark import SparkContext
sc = SparkContext()

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)

tbl_Readmissions = sqlContext.sql("Select Provider_ID, Measure_ID, Compared_to_National, Denominator, Score, Lower_Estimate, Higher_Estimate, Footnote From tbl_Readmissions_RAW where Score <> 'Not Available'").rdd

tbl_Readmissions.saveAsTextFile("/user/w205/hospital_compare_txt_TRANSFORMED/txt_readmissions/")
sqlContext.createDataFrame(tbl_Readmissions).write.parquet("/user/w205/hospital_compare_tbl_TRANSFORMED/tbl_readmissions/")

exit()
