register file:/home/hadoop/lib/pig/piggybank.jar;

--load in the weather data from the s3 bucket
weatherData = load 's3://cs440-climate/gsod/' as (line: chararray);

--separate out the fields of station, wban, year, month and temp
theData = foreach weatherData generate
  (int)TRIM(SUBSTRING(line, 0, 6)) as stn,
  (int)TRIM(SUBSTRING(line, 7, 12)) as wban,
  (int)TRIM(SUBSTRING(line, 14, 18)) as year,
  (int)TRIM(SUBSTRING(line, 18,20)) as month,
  (double)TRIM(SUBSTRING(line, 24, 30)) as temp;

--take out when station or wban is null
data_filtered = FILTER theData by stn IS NOT NULL AND wban IS NOT NULL;

-- group the data by each station with the months
grouped_dataM = group data_filtered by (stn, wban, year,month);
-- group the data by each station
grouped_dataY = group data_filtered by (stn, wban, year);

--generate average tempature for each month for each year for each station
month_data = foreach grouped_dataM generate
flatten(group), ROUND(AVG(data_filtered.temp)*100.0)/100.0 as aveM;
--generate average tempature for each year for each station
year_data = foreach grouped_dataY generate
flatten(group), ROUND(AVG(data_filtered.temp)*100.0)/100.0 as aveY;

-- find the earliest and latest year a station existed 
f_data = group data_filtered by (stn, wban);
minMaxYear = foreach f_data generate
flatten(group), MAX(data_filtered.year) as max, MIN(data_filtered.year) as min;
-- only keep stations the have been around before 1950 and after 2000
f_minMax = filter minMaxYear by min <1950 AND  max >2000 ;

-- join the stations that meet the length requirement to the year and month temps data
filtered_years = join f_minMax by (stn, wban), year_data by (stn,wban); 
filtered_months = join f_minMax by (stn, wban), month_data by (stn,wban);

-- dont look at years before 1949 for any station to make a fair comparisons
dropBeforeYear = filter filtered_years by year_data::group::year >1949;
dropBeforeMonth = filter filtered_months by month_data::group::year >1949;

-- rename all the fields to more friendly names for the years and months
final_years = foreach dropBeforeYear generate
f_minMax::group::stn as stn, f_minMax::group::wban as wban, year_data::group::year as year, year_data::aveY as aveY;
final_months = foreach dropBeforeMonth generate
f_minMax::group::stn as stn, f_minMax::group::wban as wban, month_data::group::year as year,  month_data::group::month as month, month_data::aveM as aveM;

--group by year
sumYearGroup = group final_years by year;
--find average tempature for each year
sumYear = foreach sumYearGroup generate
flatten(group),ROUND(AVG(final_years.aveY)*100.0)/100.0 as totalAve;

--group by year and month
sumMonthGroup = group final_months by (year, month);
--find the average for each month in a given year
sumMonth = foreach sumMonthGroup generate
flatten(group), ROUND(AVG(final_months.aveM)*100.0)/100.0 as totalAve;
or_sumMonth = order sumMonth by month, year;

--get station names and longs from the file in s3
stations = load 's3://cs440-climate/ish-history.txt' as (row: chararray);

--only get the stations with latitudes and longitudes
stationsFilterNull = filter stations by (row matches '.*\\d{5}.*\\d{6}.*');

-- get station number, wban, name, country, latitude, longitude from ish-history.txt
theStations = FOREACH stationsFilterNull GENERATE
(int)TRIM(SUBSTRING(row, 0, 6)) as stn,
(int)TRIM(SUBSTRING(row, 7, 12)) as wban,
TRIM(SUBSTRING(row,13, 42)) as name,
TRIM(SUBSTRING(row,43,45)) as cntry,
TRIM(SUBSTRING(row, 58, 64)) as lat,
TRIM(SUBSTRING(row, 65, 72)) as lon;

--get country list from s3
countries = load 's3://pig-test-jf/country-list.txt' as (row: chararray);

theCountries = FOREACH countries GENERATE
TRIM(SUBSTRING(row, 0, 2)) as cntry,
TRIM(SUBSTRING(row, 12, 90)) as country;

joined_country = join theStations by cntry, theCountries by cntry;
--generate friendlier names so we can join easier
cleanedStations = foreach joined_country generate
theStations::stn as stn, theStations::wban as wban, theStations::name as name, theCountries::country as country,
theStations::lat as lat, theStations::lon as lon; 

--join the two tables on the station number and wban to get complete data
final_reportYears = join final_years by (stn,wban), cleanedStations by (stn,wban);
--put in order of year starting with 1950
final_reportYears_ordered = ORDER final_reportYears BY final_years::stn ASC, final_years::wban ASC, final_years::year ASC;

-- save all of the output data to csv files
store final_reportYears_ordered into 's3n://pig-test-jf/Output/1950results/years'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'WINDOWS');

store sumYear into 's3n://pig-test-jf/Output/1950results/SumYear'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'WINDOWS');

store or_sumMonth into 's3n://pig-test-jf/Output/1950results/SumMonth'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'WINDOWS');


