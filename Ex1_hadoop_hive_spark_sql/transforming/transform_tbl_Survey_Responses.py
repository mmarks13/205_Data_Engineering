from pyspark import SparkContext
sc = SparkContext()

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)

import pandas as pd

tbl_Survey_Responses = sqlContext.sql("Select Provider_ID, Communication_with_Nurses_Achievement_Points,Communication_with_Nurses_Improvement_Points, Communication_with_Nurses_Dimension_Score, Communication_with_Doctors_Achievement_Points, Communication_with_Doctors_Improvement_Points, Communication_with_Doctors_Dimension_Score, Responsiveness_of_Hospital_Staff_Achievement_Points, Responsiveness_of_Hospital_Staff_Improvement_Points, Responsiveness_of_Hospital_Staff_Dimension_Score, Pain_Management_Achievement_Points, Pain_Management_Improvement_Points, Pain_Management_Dimension_Score, Communication_about_Medicines_Achievement_Points, Communication_about_Medicines_Improvement_Points, Communication_about_Medicines_Dimension_Score, Cleanliness_and_Quietness_of_Hospital_Environment_Achievement_Points, Cleanliness_and_Quietness_of_Hospital_Environment_Improvement_Points, Cleanliness_and_Quietness_of_Hospital_Environment_Dimension_Score, Discharge_Information_Achievement_Points, Discharge_Information_Improvement_Points, Discharge_Information_Dimension_Score, Overall_Rating_of_Hospital_Achievement_Points, Overall_Rating_of_Hospital_Improvement_Points, Overall_Rating_of_Hospital_Dimension_Score, HCAHPS_Base_Score, HCAHPS_Consistency_Score From tbl_Survey_Responses_RAW")

df_Survey_Responses = tbl_Survey_Responses.toPandas()

df_Survey_Responses = df_Survey_Responses.iloc[1:]
def Calculate_Points(x):
	try:
		if len(x.split(' ')) > 2:
			return float(x.split(' ')[0])/float(x.split(' ')[-1])
		else:
			return float(x.split(' ')[0])
	except ValueError:
		return ''

for i in range(1,len(df_Survey_Responses.columns)):
	df_Survey_Responses.iloc[0:,i] = df_Survey_Responses.iloc[0:,i].map(Calculate_Points) 

SparkDF_Survey_Responses = sqlContext.createDataFrame(df_Survey_Responses).rdd

SparkDF_Survey_Responses.saveAsTextFile("/user/w205/hospital_compare_txt_TRANSFORMED/txt_survey_responses/")
sqlContext.createDataFrame(SparkDF_Survey_Responses).write.parquet("/user/w205/hospital_compare_tbl_TRANSFORMED/tbl_survey_responses/")

exit()
