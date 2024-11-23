# Big Data Coding Exercise:  Housing

## Overview

In this exercise, you will build a solution that does the following:

- Reads a CSV file containing typical house value for various cities between 
2000 and 2024
- Parses the data to determine the oldest home value and the newest home value
for each city
- Finds the city with the largest percentage increase in home value
- Finds the city with the smallest percentage increase (or biggest 
percentage decrease) in home value
- Writes the city with the largest increase and the city with the smallest 
increase to an output file (or files)

## Assumptions
- (discussed/confirmed via email) RegionId is the primary key. Each city has a unique RegionId
- (discussed/confirmed via email) The data range, currently from 2000-01-31 to 2024-02-29, will not expand into future months
- (informed via email) 'city' is the only value allowed in the RegionType column 
- Ignore rows with blanks/nulls/empty home-values.
- The %age increase/decrease can be calculated by comparing the min/max home-values during the date-range OR by comparing the home-values between oldest/newest dates (as mentioned in the 'Overview' section above). Both approaches were coded.

## Scalability
- The solution is based on Apache Spark (coded in PySpark). 
- Spark's combination of distributed processing, fault tolerance, scalability make it an excellent choice for building scalable data engineering solutions that can handle large datasets efficiently.

## How to run ?
- cd <directory-containing-the-code>
- spark-submit --conf spark.driver.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4' --conf spark.executor.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4' end_dates_analyze_homevalues_pyspark.py home_data/homevalues_original.csv
- output files will be available in the following folders: largest_increase.csv, smallest_increase.csv


## The Input Data

The input CSV file is located in the `input` directory.  This file comes from
the [Zillow Research Housing Data](https://www.zillow.com/research/data/) site.
Specifically, this is the "ZHVI All Homes (SFR, Condo/Co-op) Time Series,
Smoothed, Seasonally Adjusted" cut of their data, and only includes the "Cities"
geography type.  

The file contains lots of columns, but only a few are of interest to this 
exercise:

- RegionName: The name of the city
- All columns after index "8" are the monthly home values for the city
- The first non-empty column after index "8" is the oldest home value 
for the city
- The very last column is the newest home value for the city

## The Output Format

You may choose to write your output in any format you like to a set of files in
the "output" directory.  Any common big data format is acceptable, as is a
CSV file or a TXT file.  The output for both the minimum and maximum changes
should include the following:

- CityName: The name of the city
- The oldest home value for that city
- The newest home value for that city
- The percentage increase or decrease in home value

## Example Input and Output

To illustrate the requirements above, let's consider a simple example where the
input csv file contains only the following columns and rows:

```csv
RegionName,2024-04-30,2024-05-31,2024-06-30,2024-07-31,2024-08-31
Alfa City,210,200,220,220,215
Bravotown,,,300,320,330
Charlieburg,,100,150,250,300
Delta Hills,200,210,230,240,250
```

If that were the input data, then the output might be something like this, 
which identifies Charlieburg as the city with the largest increase and 
Alfa City as the city with the smallest increase:

```csv
CityName,OldestHomeValue,NewestHomeValue,PercentageChange
Charlieburg,100,300,+200%
Alfa City,210,215,2.38%
```

