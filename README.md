# INFO 602 Final Mini Project: Virginia Overdose ED Visits and Socioeconomic Indicators

## Project Proposition

This project investigates whether Virginia localities with higher poverty rates and lower educational attainment tend to have higher drug overdose emergency department (ED) visit rates.

## Datasets Used

1. **VDH PUD Overdose ED Visits by Year and Geography**
   - Used 2025 locality-level records
   - Filtered to All Drug overdose ED visits
   - Used overdose ED visit rate per 10,000 visits

2. **U.S. Census ACS 2024 5-Year Table B17018**
   - Poverty Status in the Past 12 Months of Families by Household Type by Educational Attainment of Householder
   - Used county/city-level Virginia records
   - Derived poverty rate and share of families where the householder had high school education or less

## Tools

- Google Cloud Dataproc
- Apache Spark / PySpark
- Spark DataFrame / Spark SQL API
- HDFS for distributed file storage

## Cleaning and Join Strategy

The VDH dataset includes some combined localities. To avoid joining combined overdose geography records to single ACS county/city records, this analysis used only VDH rows where:

- Year = 2025
- Drug Type = All Drug
- Geography Level = Locality
- Combined Locality = No

The VDH and ACS datasets were joined using Virginia FIPS codes.

## Model-Free Analysis

No regression or machine learning model was used. The analysis relies on:

- grouped aggregations
- socioeconomic buckets
- average overdose ED visit rates
- top locality ranking

## Main Outputs

- `final_joined_dataset.csv`
- `poverty_summary.csv`
- `education_summary.csv`
- `top_overdose_localities.csv`

## Summary of Findings

The poverty bucket analysis showed a clear increase in average overdose ED visit rates as poverty increased. Localities in the lowest poverty bucket had an average overdose ED visit rate of 32.46, while localities in the very high poverty bucket had an average rate of 55.12.

The education bucket analysis showed a weaker but generally positive relationship. Localities with the lowest share of families whose householder had high school education or less had an average overdose ED visit rate of 34.99, while the very high HS-or-less bucket had an average rate of 40.16.

Overall, the analysis provides stronger support for the poverty portion of the proposition than for the educational attainment portion.
