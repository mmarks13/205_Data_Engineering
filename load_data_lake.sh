wget https://data.medicare.gov/views/bg9k-emty/files/Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotG bDHHW_s?content_type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip
mv Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s\?content_type\=application%2Fzip\;\ charse t\=binary Hospital_Data
mkdir Ex1_Hospital_Data
mv Hospital_Data Ex1_Hospital_Data/Hospital_Data
unzip Hospital_Data
tail -n +2 *.csv > *.csv
for file in *.csv; do mv "$file" "${file//[[:space:]]}"; done
mv HospitalGeneralInformation.csv hospitals.csv
mv TimelyandEffectiveCare-Hospital.csv effective_care.csv
mv ReadmissionsandDeaths-Hospital.csv readmissions.csv
mv hvbp_hcahps_05_28_2015.csv survey_responses.csv

