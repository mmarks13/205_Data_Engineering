#if you are the root user, switch to w205 user
if (( $EUID = 0 )); then
    su - w205
fi

#make sure we are in the home directory
cd

#download the file
wget https://data.medicare.gov/views/bg9k-emty/files/Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s?content_ type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip

#rename the file in its existing directory. 
mv Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s\?content_type\=application%2Fzip\;\ charset\=binary Hospital_Data

#Create a folder for all the unzipped files
mkdir Ex1_Raw_Hospital_Data

#move the zip file there
mv Hospital_Data Ex1_Raw_Hospital_Data

#change directories
cd Ex1_Raw_Hospital_Data/

#unzip the file
unzip Hospital_Data 

#remove the spaces in the filenames
for file in *.csv; do mv $file ${file//[[:space:]]}; done

#rename our tables of interest
mv HospitalGeneralInformation.csv hospitals.csv
mv TimelyandEffectiveCare-Hospital.csv effective_care.csv
mv ReadmissionsandDeaths-Hospital.csv readmissions.csv
mv hvbp_hcahps_05_28_2015.csv survey_responses.csv
mv MeasureDates.csv measure_dates.csv

#make HDFS directory for this exercise
hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -mkdir /user/w205/hospital_compare/tbl_hospitals
hdfs dfs -mkdir /user/w205/hospital_compare/tbl_effective_care
hdfs dfs -mkdir /user/w205/hospital_compare/tbl_readmissions
hdfs dfs -mkdir /user/w205/hospital_compare/tbl_survey_responses
hdfs dfs -mkdir /user/w205/hospital_compare/tbl_measure_dates

#Move our tables of interest into HDFS
hdfs dfs -put Ex1_Raw_Hospital_Data/hospitals.csv /user/w205/hospital_compare/tbl_hospitals/hospitals
hdfs dfs -put Ex1_Raw_Hospital_Data/effective_care.csv /user/w205/hospital_compare/tbl_effective_care/effective_care
hdfs dfs -put Ex1_Raw_Hospital_Data/readmissions.csv /user/w205/hospital_compare/tbl_readmissions/readmissions
hdfs dfs -put Ex1_Raw_Hospital_Data/survey_responses.csv /user/w205/hospital_compare/tbl_survey_responses/survey_responses
hdfs dfs -put Ex1_Raw_Hospital_Data/measure_dates.csv /user/w205/hospital_compare/tbl_measure_dates/measure_dates
