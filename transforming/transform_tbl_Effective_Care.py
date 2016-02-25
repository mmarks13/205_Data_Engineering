from pyspark import SparkContext
sc = SparkContext()

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)

tbl_Effective_Care = sqlContext.sql("Select Provider_ID, Measure_ID, Score, Sample, Footnote From tbl_Effective_Care_RAW where Score <> 'Not Available'").rdd

tbl_Effective_Care.saveAsTextFile("/user/w205/hospital_compare_TRANSFORMED/tbl_effective_care/")

exit()
