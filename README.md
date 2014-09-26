ClimateAWS
==========

<b>Website showing final results<b>
http://cs440-jf.s3-website-us-east-1.amazonaws.com/

<b>Final project for WVU CS440 Databases<b>
===========
The goal was to analyze the weather data from this public data set on AWS http://aws.amazon.com/datasets/Climate/2759
I used Amazon Elastic MapReduce which uses Hadoop to process the data.

First I wrote a PIG script (Code/climateData1950.pig) to do the queries to get the required data and output to CSV files which are found in the Data folder.

Next I analyzed the CSV files with a python script (Code/analyze.py). The python script did least squares analyis to determine if the tempature was increasing or decreasing over the years for each weather station.
The python script also outputed graphs that are found in the Images folder showing the results.

Finally I used a python script (Code/generateJSmap.py) that created a map in JavaScript showing the individual stations and the results of warming or cooling for that station. The map is show on http://cs440-jf.s3-website-us-east-1.amazonaws.com/
