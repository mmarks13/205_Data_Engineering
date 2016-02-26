from pyspark import SparkContext
sc = SparkContext()

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)

import pandas as pd

tbl_hospitals = sqlContext.read.parquet("/user/w205/hospital_compare_tbl_TRANSFORMED/tbl_hospitals/*parquet")
tbl_hospitals.registerTempTable("tbl_hospitals")

tbl_effective_care = sqlContext.read.parquet("/user/w205/hospital_compare_tbl_TRANSFORMED/tbl_effective_care/*parquet")
tbl_effective_care.registerTempTable("tbl_effective_care")

tbl_readmissions = sqlContext.read.parquet("/user/w205/hospital_compare_tbl_TRANSFORMED/tbl_readmissions/*parquet")
tbl_readmissions.registerTempTable("tbl_readmissions")



Effective_Care_Measure_Mean_StdDev = sqlContext.sql("Select Measure_ID, avg(Score) as Mean, stddev(Score) as StdDev From tbl_effective_care group by Measure_ID")
Readmissions_Measure_Mean_StdDev = sqlContext.sql("Select Measure_ID, avg(Score) as Mean, stddev(Score) as StdDev From tbl_readmissions group by Measure_ID")

Effective_Care_Measures_By_Provider_ID = sqlContext.sql("Select Provider_ID, Measure_ID, avg(Score) as Score From tbl_effective_care group by Provider_ID, Measure_ID")
Readmissions_Measures_By_Provider_ID = sqlContext.sql("Select Provider_ID, Measure_ID, avg(Score) as Score From tbl_readmissions group by Provider_ID, Measure_ID")

All_Measures_By_Provider_ID = Effective_Care_Measures_By_Provider_ID.where("Score >=0").unionAll(Readmissions_Measures_By_Provider_ID.where("Score >=0"))

All_Measures_Mean_StdDev = Effective_Care_Measure_Mean_StdDev.where("StdDev >=0").unionAll(Readmissions_Measure_Mean_StdDev.where("StdDev >=0"))

All_Measures_By_Provider_ID_Z_Scores_STAGE = All_Measures_By_Provider_ID.join(All_Measures_Mean_StdDev, "Measure_ID")

All_Measures_By_Provider_ID_Z_Scores = All_Measures_By_Provider_ID_Z_Scores_STAGE.selectExpr('Provider_ID', 'Measure_ID',"(Score-Mean)/StdDev  as Z_Score")

All_Measures_Z_Scores_By_Hospital = All_Measures_By_Provider_ID_Z_Scores.join(tbl_hospitals, "Provider_ID").selectExpr('Hospital_Name', 'Measure_ID','Z_Score')

Measure_Count_By_Hospital = All_Measures_Z_Scores_By_Hospital.groupBy('Hospital_Name').count()

Avg_Z_Scores_By_Hospital = All_Measures_Z_Scores_By_Hospital.groupBy('hospital_name').avg('Z_Score').join(Measure_Count_By_Hospital,'hospital_name').where("count > 20")
Avg_Z_Scores_By_Hospital.cache()

Top_Ten_Hospitals = Avg_Z_Scores_By_Hospital.orderBy(['avg(Z_Score)', 'hospital_name'], ascending=[0, 1]).limit(10)
Top_Ten_Hospitals.cache()

Top_Ten_Hospitals.select('hospital_name').toPandas().to_csv("/user/w205/hospital_compare_INVESTIGATIONS/best_hospitals")

