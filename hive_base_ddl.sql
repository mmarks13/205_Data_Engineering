DROP TABLE tbl_Hospitals
CREATE EXTERNAL TABLE tbl_Hospitals
(
Provider_ID varchar(50),
Hospital_Name string,
Address string,
City string,
State string,
ZIP_Code bigint,
County_Name string,
Phone_Number string,
Hospital_Type string,
Hospital_Ownership string,
Emergency_Services string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = '"',
   "escapeChar"    = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/hospitals';


DROP TABLE tbl_Effective_Care
CREATE EXTERNAL TABLE tbl_Effective_Care
(
Provider_ID varchar(50),
Hospital_Name string,
Address string,
City string,
State string,
ZIP_Code bigint,
County_Name string,
Phone_Number string,
Condition string,
Measure_ID string,
Measure_Name string,
Score float,
Sample float,
Footnote string,
Measure_Start_Date date,
Measure_End_Date date
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = '"',
   "escapeChar"    = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/effective_care';

DROP TABLE tbl_Readmissions
CREATE EXTERNAL TABLE tbl_Readmissions
(
Provider_ID varchar(50),
Hospital_Name string,
Address string,
City string,
State string,
ZIP_Code bigint,
County_Name string,
Phone_Number string,
Measure_Name string,
Measure_ID string, 
Compared_to_National string,
Denominator float,
Score float,
Lower_Estimate float,
Higher_Estimate float,
Footnote string,
Measure_Start_Date date,
Measure_End_Date date
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = '"',
   "escapeChar"    = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/tbl_Readmissions';


DROP TABLE tbl_Survey_Responses
CREATE EXTERNAL TABLE tbl_Survey_Responses
(
Provider_ID varchar(50),
Hospital_Name string,
Address string,
City string,
State string,
ZIP_Code bigint,
County_Name string,
Communication_with_Nurses_Achievement_Points string,
Communication_with_Nurses_Improvement_Points string,
Communication_with_Nurses_Dimension_Score string,
Communication_with_Doctors_Achievement_Points string,
Communication_with_Doctors_Improvement_Points string,
Communication_with_Doctors_Dimension_Score string,
Responsiveness_of_Hospital_Staff_Achievement_Points string,
Responsiveness_of_Hospital_Staff_Improvement_Points string,
Responsiveness_of_Hospital_Staff_Dimension_Score string,
Pain_Management_Achievement_Points string,
Pain_Management_Improvement_Points string,
Pain_Management_Dimension_Score string,
Communication_about_Medicines_Achievement_Points string,
Communication_about_Medicines_Improvement_Points string,
Communication_about_Medicines_Dimension_Score string,
Cleanliness_and_Quietness_of_Hospital_Environment_Achievement_Points string,
Cleanliness_and_Quietness_of_Hospital_Environment_Improvement_Points string,
Cleanliness_and_Quietness_of_Hospital_Environment_Dimension_Score string,
Discharge_Information_Achievement_Points string,
Discharge_Information_Improvement_Points string,
Discharge_Information_Dimension_Score string,
Overall_Rating_of_Hospital_Achievement_Points string,
Overall_Rating_of_Hospital_Improvement_Points string,
Overall_Rating_of_Hospital_Dimension_Score string,
HCAHPS_Base_Score float,
HCAHPS_Consistency_Score float
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = '"',
   "escapeChar"    = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/survey_responses';


DROP TABLE tbl_Measure_Dates
CREATE EXTERNAL TABLE tbl_Measure_Dates
(
Measure_Name string,
Measure_ID string,
Measure_Start_Quarter string,
Measure_Start_Date timestamp,
Measure_End_Quarter string,
Measure_End_Date timestamp
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = '"',
   "escapeChar"    = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/measure_dates';