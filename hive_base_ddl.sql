/* Using varchar for data type to account for unknown data issues. */

DROP TABLE Hospitals;
CREATE EXTERNAL TABLE Hospitals (ProviderID, varchar(500)
,Hospital_Name varchar(500)
,Address, varchar(500)
,City, varchar(500)
,State, varchar(500)
,ZIP_Code, varchar(500)
,County_Name, varchar(500)
,Phone_Number, varchar(500)
,Hospital_Type, varchar(500)
,Hospital_Ownership, varchar(500)
,Emergency_Services, varchar(500))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘,’
STORED AS TEXTFILE
LOCATION ‘./Ex1_Hospital_Data/Tables’;

DROP TABLE Effective_Care;
CREATE EXTERNAL TABLE Effective_Care (ProviderID, varchar(500)
,Hospital_Name varchar(500)
,Address, varchar(500)
,City, varchar(500)
,State, varchar(500)
,ZIP_Code, varchar(500)
,County_Name, varchar(500)
,Phone_Number, varchar(500)
,Condition, varchar(500)
,Measure_ID, varchar(500)
,Measure_Name, varchar(500)
,Condition, varchar(500)
,Score, varchar(500)
,Sample, varchar(500)
,Footnote, varchar(500)
,Meaure_Start_Date, varchar(500)
,Measure_End_Date, varchar(500))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘,’
STORED AS TEXTFILE
LOCATION ‘./Ex1_Hospital_Data/Tables’;


DROP TABLE Readmissions;
CREATE EXTERNAL TABLE Readmissions (ProviderID, varchar(500)
,Hospital_Name varchar(500)
,Address, varchar(500)
,City, varchar(500)
,State, varchar(500)
,ZIP_Code, varchar(500)
,County_Name, varchar(500)
,Phone_Number, varchar(500)
,Measure_Name, varchar(500)
,Measure_ID, varchar(500)
,Compared_To_National, varchar(500)
,Denominator, varchar(500)
,Score, varchar(500)
,Lower_Estimate, varchar(500)
,Higher_Estimate, varchar(500)
,Footnote, varchar(500)
,Measure_Start_Date, varchar(500)
,Measure_End_Date, varchar(500))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘,’
STORED AS TEXTFILE
LOCATION ‘./Ex1_Hospital_Data/Tables’;


DROP TABLE Survey_Results;
CREATE EXTERNAL TABLE Survey_Results (ProviderID, varchar(500)
,Hospital_Name varchar(500)
,Address, varchar(500)
,City, varchar(500)
,State, varchar(500)
,ZIP_Code, varchar(500)
,County_Name, varchar(500)
,Nurse_Comm_Achieve_Pts, varchar(500)
,Nurse_Comm_Improve_Pts, varchar(500)
,Nurse_Comm_Dimension_Score, varchar(500)
,Doctor_Comm_Achieve_Pts, varchar(500)
,Doctor_Comm_Improve_Pts, varchar(500)
,Doctor_Comm_Dimension_Score, varchar(500)
,Staff_Response_Achieve_Pts, varchar(500)
,Staff_Response_Improvement_Pts, varchar(500)
,Staff_Response_Dimension_Score, varchar(500)
,Pain_Mgmt_Achieve_Pts, varchar(500)
,Pain_Mgmt_Improve_Pts, varchar(500)
,Pain_Mgmt_Dimension_Score, varchar(500)
,Med_Comm_Achieve_Pts, varchar(500)
,Med_Comm_Improve_Pts, varchar(500)
,Med_Comm_Dimension_Score, varchar(500)
,Clean_Quiet_Comm_Achieve_Pts, varchar(500)
,Clean_Quiet_Improve_Pts, varchar(500)
,Clean_Quiet_Dimension_Score, varchar(500)
,Discharge_Info_Achieve_Pts, varchar(500)
,Discharge_Info_Improve_Pts, varchar(500)
,Discharge_Info_Dimension_Score, varchar(500)
,Overall_Achieve_Pts, varchar(500)
,Overall_Improve_Pts, varchar(500)
,Overall_Dimension_Score, varchar(500)
,HCAHPS_Base_Score, varchar(500)
,HCAHPS_Consistency_Score, varchar(500))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘,’
STORED AS TEXTFILE
LOCATION ‘./Ex1_Hospital_Data/Tables’;

