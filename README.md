NASDAQ DATA WAREHOUSE
=====================

This project is a proof of concept for a Data Warehouse of NASDAQ stock data. There are a few available datasets out there that include historical data of securites. This project leverages those existing dataset and enriches it with demographics data and more information on the security and company. The data is modeled into an easy to use star schema. The hope is that data scientists, business intelligence analysts and data analysts can query this dataset to draw insights and build machine learning models.

Since this is a proof of concept, some of the sourced data is not **real** data but rather test data queried from a financial data provider (more info is available in the data sources section)

DATA SOURCES
============

1. **Stock Market Dataset**: This data is publicly availabe on Kaggle and can be found [here](https://www.kaggle.com/jacksoncrow/stock-market-dataset). It contains historical daily prices for all tickers currently trading on NASDAQ

2. **Demographics Dataset**: This data comes from OpenSoft. It contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).

3. **IEX Cloud Financial Data**: The two datasets below are downloaded using an API from [IEX Cloud](https://iexcloud.io/), a financial data provider. Please note that I used the test API so as to avoid incurring unecessary costs.
    - **Company Profile**: This dataset provides key information about the company that issued the security as well as additional information about the security itself. You can read more about the data element [here](https://iexcloud.io/docs/api/#company) Please note that the dataset provided in the repo was filled with demographics data because the API return randomized strings so as to allow to connect to the demographics table. 
    - **Company Key Statistics**: This dataset enriches the comapny profile with financial statistics on the company and the security. You can read more about it [here](https://iexcloud.io/docs/api/#key-stats)
    
DATA MODEL
============

I chose a simple to use star schema with the grain (daily quotes)  as granular as possible. The intent is to provide consumers the flexibility to slice, dice and aggregate the data however they want so that they can generate the reports and/or mathematical models they want. As a result, I avoided aggregating the data.

Here are the dimensional tables. Please consult the data dictionary for more information on these tables.

Facts Table
-----------

1. **daily_quotes_fact**: The grain is daily quotes for all the securities avaiable in this dataset.
    - *quote_id, symbol, company_id, date, demographic_id, open, high, low, close, adj_close, volume*
    
Dimensions Table
----------------

2. **security_dim**: Includes all available information and statistics on the security.
    - *symbol, primary_sic_code, security_name, company_name, issue_type, exchange, market_cap, week52change, week52high, week52low, shares_outstanding, maxChangePercent, year5ChangePercent, year2ChangePercent, year1ChangePercent, ytdChangePercent, month6ChangePercent, month3ChangePercent, month1ChangePercent, day30ChangePercent, day5ChangePercent, nextdividenddate, dividendyield, nextearningsdate, exdividenddate, peratio, beta*


3. **company_dim**: Includes all available information and on the issuing company.
    - *company_id, company_name, industry, website, description, ceo, sector, employees, address, address2, state, city, zip, country, phone*
    
    
4. **demographics_dim**: Contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.
    - *demographic_id, city, state, median_age, male_population, female_population, total_population, number_veterans, foreign_born, avg_household_size, state_code, race, count*


5. **time_dim**: Standard time dimension table.
    - *date, day, week, month, year, weekday*
    
DATA DICTIONARY
===============

1. **daily_quotes_fact**


Field Name    | Data Type     | Description     
------------- |-------------  | -------------
quote_id      | Integer       | Unique fact ID
symbol        | Text          | Security NASDAQ Symbol
company_id    | Integer       | Company ID identifier
date          | Date          | Date of the quote
demographic_id| Text          | Security NASDAQ Symbol
open          | Float         | Opening price
high          | Float         | Highest price in the day
low           | Float         | Lowest price in the day
close         | Float         | Closing price
adj_close     | Float         | Adjusted closing price


2. **security_dim**


Field Name        | Data Type     | Description     
-------------    |-------------  | -------------
symbol           | Text          | Security NASDAQ Symbol
primary_sic_code | Integer       | Primary [SIC Code](https://en.wikipedia.org/wiki/Standard_Industrial_Classification) for the symbol (if available)
security_name    | Text          | Name of the security
company_name     | Text          | Name of the company
issue_type       | Text          | refers to the common issue type of the stock.
exchange         | Text          | Exchange it is trading on
market_cap       | Float         | Market cap of the security calculated as shares outstanding * previous day close.
week52change     | Float         | Percentage change
week52high       | Float         |
week52low        | Float         |
shares_outstanding | Integer     | Number of shares outstanding as the difference between issued shares and treasury shares. [Investopedia](https://www.investopedia.com/terms/o/outstandingshares.asp)
maxChangePercent | Float         |
year5ChangePercent | Float       |
year2ChangePercent | Float       |
year1ChangePercent | Float       |
ytdChangePercent   | Float       |
month6ChangePercent | Float      |
month3ChangePercent | Float      |
month1ChangePercent | Float      |
day30ChangePercent  | Float      |
day5ChangePercent  | Float      |
nextdividenddate   | Date       | Expected ex date of the next dividend
dividendyield      | Float      | The ratio of trailing twelve month dividend compared to the previous day close price. The dividend yield is represented as a percentage calculated as (ttmDividendRate) / (previous day close price) [Investopedia](https://www.investopedia.com/terms/d/dividendyield.asp)
nextearningsdate   | Date       | Expected next earnings report date
exdividenddate     | Text       | Ex date of the last dividend
peRatio            | Float      | Price to earnings ratio calculated as (previous day close price) / (ttmEPS)
beta               | Float      | Beta is a measure used in fundamental analysis to determine the volatility of an asset or portfolio in relation to the overall market. Levered beta calculated with 1 year historical data and compared to SPY.


3. **companies_dim**


Field Name    | Data Type     | Description     
------------- |-------------  | -------------
company_id    | Integer       | Unique Company ID
company_name  | Text          | Name of the company
industry      | Text          | Industry of the company
website       | Text          | Company website
Description   | Text          | 
CEO           | Text          |
employees     | Integer       | Number of employees
address       | Text          | Street address of the company if available
address2      | Text          | Street address of the company if available
state         | Text          | State of the company if available
city          | Text          | City of the company if available
zip           | Text          | Zip of the company if available
country       | Text          | Country of the company if available
phone         | Text          | Phone number of the company if available


4. **demographics_dim**


Field Name    | Data Type     | Description     
------------- |-------------  | -------------
demographic_id | Integer      | Unique Demographic ID
city          | Text          | 
state         | Text          |
median_age    | Float         |
male_population | Integer     | 
female_population | Integer     |
total_population | Integer     |
number_verteras | Integer     | Number of veterans in the city
foreign_bon     | Integer     | Number of veterans in the city
avg_household_size | Float    | Average of people in a household
state_code    | Text          | 
race          | Text          | List of ethnic races in the city
count         | Integer       |

Tools Used
==========

1. **Python**: The ELT pipeline was processed with the PyData stack (Pandas). Given its relatively small size (approximately 3 GB), using big data analytics tools like Spark would likely hinder performance

2. **PostgreSQL**: Postgres was the ideal solution for this use case, given the relatively small size of the data. It is open source and offers lots of flexibility to create a star schema. With future enhancements, I may consider migrating the database to a columnar oriented database (some extensions for Postgres exists and/or Redshift).

Additional Consideration
========================

1. **The data was increased by 100x.**

The IEX Cloud API offers live data (updated every second). I would like to leverage this but this will mean I will need more big data oriented tools like Spark for data processing and Redshift for data warehousing.

2. **Automated Data Pipeline**

To be able to provide to functionality of updating the database with new data on a given cadence (e.g. daily, hourly etc..), a proper scheduler would be required. Airflow seems like a perfect choice. Future enhancements will leverage the powerful abilities of airflow. 

3. **Multi-User Access**

Theoretically, Postgres can be accessed by [as much as 115 people](https://cloud.ibm.com/docs/databases-for-postgresql?topic=databases-for-postgresql-managing-connections). However, in more extreme scenarios, an MPP database like Redshift might be a better choice.

How to Run
===========

1. Ensure PostgreSQL is configured in your local environment
2. Input the required configuration details in **dwh.cfg**
3. Download the stock-market-dataset and unzip to **stock-market-dataset/data**
4. run **etl.py**

Enjoy your new NASDAQ Data Warehouse! 