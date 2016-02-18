#if you are the root user, switch to w205 user
if (( $EUID == 0 )); then
    su - w205
fi

#delete existing directory that this code creates. 
rm -rf /home/w205/Ex1_Raw_Hospital_Data/


#Create a folder for all the unzipped files
mkdir /home/w205/Ex1_Raw_Hospital_Data


#download the file save a Hospital_Data in home directory
wget https://data.medicare.gov/views/bg9k-emty/files/Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s?content_ type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip -O /home/w205/Ex1_Raw_Hospital_Data


#move the zip file there
mv /home/w205/Ex1_Raw_Hospital_Data/Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s?content_ /home/w205/Ex1_Raw_Hospital_Data/Hospital_Data

#unzip the file
unzip /home/w205/Ex1_Raw_Hospital_Data/Hospital_Data 

#remove the spaces in the filenames
for file in *.csv; do mv $file ${file//[[:space:]]}; done

#rename our tables of interest
mv /home/w205/Ex1_Raw_Hospital_Data/HospitalGeneralInformation.csv /home/w205/Ex1_Raw_Hospital_Data/hospitals.csv
mv /home/w205/Ex1_Raw_Hospital_Data/TimelyandEffectiveCare-Hospital.csv /home/w205/Ex1_Raw_Hospital_Data/effective_care.csv
mv /home/w205/Ex1_Raw_Hospital_Data/ReadmissionsandDeaths-Hospital.csv /home/w205/Ex1_Raw_Hospital_Data/readmissions.csv
mv /home/w205/Ex1_Raw_Hospital_Data/hvbp_hcahps_05_28_2015.csv /home/w205/Ex1_Raw_Hospital_Data/survey_responses.csv
mv /home/w205/Ex1_Raw_Hospital_Data/MeasureDates.csv /home/w205/Ex1_Raw_Hospital_Data/measure_dates.csv


#delete existing HDFS directory that this code creates. 
hdfs dfs -rm -r hdfs://user/w205/hospital_compare/

#make HDFS directory for this exercise
hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -mkdir /user/w205/hospital_compare/tbl_hospitals
hdfs dfs -mkdir /user/w205/hospital_compare/tbl_effective_care
hdfs dfs -mkdir /user/w205/hospital_compare/tbl_readmissions
hdfs dfs -mkdir /user/w205/hospital_compare/tbl_survey_responses
hdfs dfs -mkdir /user/w205/hospital_compare/tbl_measure_dates

#Move our tables of interest into HDFS
hdfs dfs -put /home/w205/Ex1_Raw_Hospital_Data/Ex1_Raw_Hospital_Data/hospitals.csv /user/w205/hospital_compare/tbl_hospitals/hospitals
hdfs dfs -put /home/w205/Ex1_Raw_Hospital_Data/Ex1_Raw_Hospital_Data/effective_care.csv /user/w205/hospital_compare/tbl_effective_care/effective_care
hdfs dfs -put /home/w205/Ex1_Raw_Hospital_Data/Ex1_Raw_Hospital_Data/readmissions.csv /user/w205/hospital_compare/tbl_readmissions/readmissions
hdfs dfs -put /home/w205/Ex1_Raw_Hospital_Data/Ex1_Raw_Hospital_Data/survey_responses.csv /user/w205/hospital_compare/tbl_survey_responses/survey_responses
hdfs dfs -put /home/w205/Ex1_Raw_Hospital_Data/Ex1_Raw_Hospital_Data/measure_dates.csv /user/w205/hospital_compare/tbl_measure_dates/measure_dates
