from pyspark import SparkContext
sc = SparkContext()

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
import org.apache.spark.sql.functions.stddev_pop
tbl_effective_care = sqlContext.read.parquet("/user/w205/hospital_compare_tbl_TRANSFORMED/tbl_effective_care/*parquet")
tbl_effective_care.registerTempTable("tbl_effective_care")

tbl_measure_dates = sqlContext.read.parquet("/user/w205/hospital_compare_tbl_TRANSFORMED/tbl_measure_dates/*parquet")
tbl_measure_dates.registerTempTable("tbl_measure_dates")

Effective_Care_Measure_Mean_by_Provider_ID = sqlContext.sql("Select Provider_ID, Measure_ID, avg(Score) as Mean_Score From tbl_effective_care group by Provider_ID, Measure_ID")

Effective_Care_Measure_Mean_by_Provider_ID.registerTempTable("Effective_Care_Measure_Mean_by_Provider_ID")

Std_Dev_By_Measure_ID = sqlContext.sql("Select Measure_ID, mean(Mean_Score) as mean, stddev(Mean_Score) as StdDev From Effective_Care_Measure_Mean_by_Provider_ID group by Measure_ID")
Std_Dev_By_Measure_ID.registerTempTable("Std_Dev_By_Measure_ID")

CoV_by_Measure_ID = sqlContext.sql("Select Measure_ID, mean/StdDev as CoV From Std_Dev_By_Measure_ID")

CoV_By_Measure_Name = CoV_by_Measure_ID.join(tbl_measure_dates,"Measure_ID").select('Measure_Name','CoV')

Top_Ten_Varying_Procedures = CoV_By_Measure_Name.orderBy(['CoV', 'Measure_Name'], ascending=[0, 1]).limit(10)

Top_Ten_Varying_Procedures.rdd.saveAsTextFile("/user/w205/hospital_compare_INVESTIGATIONS/hospital_variability")

