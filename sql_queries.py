# DROP TABLES

staging_stats_drop = "DROP TABLE IF EXISTS staging_stats"
staging_companies_drop = "DROP TABLE IF EXISTS staging_companies"
staging_demographics_drop = "DROP TABLE IF EXISTS staging_demographics"
staging_daily_quotes_drop = "DROP TABLE IF EXISTS staging_daily_quotes"
time_dim_drop = "DROP TABLE IF EXISTS timeDim"
security_dim_drop = "DROP TABLE IF EXISTS securityDim"

# CREATE TABLES

# Staging
staging_stats_create = ("""CREATE TABLE IF NOT EXISTS staging_stats (
                                     symbol VARCHAR,
                                     week52change FLOAT, week52high FLOAT, week52low FLOAT, marketcap FLOAT, employees FLOAT,
                                     day200MovingAvg FLOAT, day50MovingAvg FLOAT, float FLOAT, avg10Volume FLOAT, avg30Volume FLOAT,
                                     ttmEPS FLOAT, ttmDividendRate FLOAT, companyName VARCHAR, sharesOutstanding FLOAT,
                                     maxChangePercent FLOAT, year5ChangePercent FLOAT, year2ChangePercent FLOAT, year1ChangePercent FLOAT,
                                     ytdChangePercent FLOAT, month6ChangePercent FLOAT, month3ChangePercent FLOAT, month1ChangePercent FLOAT,
                                     day30ChangePercent FLOAT, day5ChangePercent FLOAT, nextdividenddate DATE, dividendyield FLOAT, nextearningsdate DATE, exdividenddate DATE,
                                     peratio FLOAT, beta FLOAT);
""")

staging_companies_create = ("""CREATE TABLE IF NOT EXISTS staging_companies (
                                    symbol VARCHAR, companyName VARCHAR, exchange VARCHAR, industry VARCHAR,
                                    website VARCHAR, description VARCHAR, CEO VARCHAR, securityName VARCHAR, issueType VARCHAR,
                                    sector VARCHAR, primarySicCode FLOAT, employees FLOAT, address VARCHAR, address2 VARCHAR,
                                    state VARCHAR, city VARCHAR, zip VARCHAR, country VARCHAR, phone VARCHAR);
""")

staging_demographics_create = ("""CREATE TABLE IF NOT EXISTS staging_demographics (
                                    city VARCHAR,
                                    state VARCHAR,
                                    median_age FLOAT,
                                    male_population FLOAT, female_population FLOAT, total_population FLOAT,
                                    number_veterans FLOAT, foreign_born FLOAT,
                                    avg_household_size FLOAT,
                                    state_code VARCHAR,
                                    race VARCHAR,
                                    count FLOAT);
""")

staging_daily_quotes_create = ("""CREATE TABLE IF NOT EXISTS staging_daily_quotes(
                                    date DATE,
                                    open FLOAT,
                                    high FLOAT,
                                    low FLOAT,
                                    close FLOAT,
                                    adj_close FLOAT,
                                    volume FLOAT,
                                    symbol VARCHAR);
""")

time_dim_create = ("""CREATE TABLE IF NOT EXISTS time_dim (date DATE PRIMARY KEY,
                                                       day int NOT NULL,
                                                       week int NOT NULL,
                                                       month int NOT NULL,
                                                       year int NOT NULL,
                                                       weekday int NOT NULL);
""")

# Facts & Dims

security_dim_create = ("""CREATE TABLE IF NOT EXISTS security_dim (
                                    symbol VARCHAR PRIMARY KEY,
                                    primary_sic_code INT,
                                    security_name VARCHAR NOT NULL,
                                    company_name VARCHAR,
                                    issue_type VARCHAR NOT NULL,
                                    exchange VARCHAR NOT NULL,
                                    market_cap BIGINT,
                                    week52change FLOAT, week52high FLOAT, week52low FLOAT,
                                    shares_outstanding BIGINT,
                                    maxChangePercent FLOAT, year5ChangePercent FLOAT, year2ChangePercent FLOAT, year1ChangePercent FLOAT,
                                    ytdChangePercent FLOAT, month6ChangePercent FLOAT, month3ChangePercent FLOAT, month1ChangePercent FLOAT,
                                    day30ChangePercent FLOAT, day5ChangePercent FLOAT, nextdividenddate DATE, dividendyield FLOAT, nextearningsdate DATE, exdividenddate DATE,
                                    peratio FLOAT, beta FLOAT
                                    )
""")
company_dim_create = ("""CREATE TABLE IF NOT EXISTS company_dim (
                                    company_id SERIAL PRIMARY KEY,
                                    company_name VARCHAR,
                                    industry VARCHAR,
                                    website VARCHAR,
                                    description VARCHAR,
                                    ceo VARCHAR,
                                    sector VARCHAR,
                                    employees BIGINT,
                                    address VARCHAR,
                                    address2 VARCHAR,
                                    state VARCHAR,
                                    city VARCHAR,
                                    zip VARCHAR,
                                    country VARCHAR,
                                    phone VARCHAR
                                )
                      
""")

demographics_dim_create = ("""CREATE TABLE IF NOT EXISTS demographics_dim (
                                    demographic_id SERIAL PRIMARY KEY,
                                    city VARCHAR,
                                    state VARCHAR,
                                    median_age FLOAT,
                                    male_population INT, female_population INT, total_population INT,
                                    number_veterans INT, foreign_born INT,
                                    avg_household_size FLOAT,
                                    state_code VARCHAR,
                                    race VARCHAR,
                                    count INT);
""")

fact_table_create = ("""CREATE TABLE IF NOT EXISTS daily_quotes_fact (
                                    quote_id SERIAL PRIMARY KEY,
                                    symbol VARCHAR,
                                    company_id INT,
                                    date DATE,
                                    demographic_id INT,
                                    open FLOAT,
                                    high FLOAT,
                                    low FLOAT,
                                    close FLOAT,
                                    adj_close FLOAT,
                                    volume INT
                        )
                     
""")

# INSERT QUERIES

security_dim_insert = ("""INSERT INTO security_dim (symbol, primary_sic_code, security_name, company_name, issue_type, exchange, market_cap,
                                                   week52change, week52high, week52low,
                                                   shares_outstanding,
                                                   maxChangePercent, year5ChangePercent, year2ChangePercent, year1ChangePercent,
                                                   ytdChangePercent, month6ChangePercent, month3ChangePercent, month1ChangePercent,
                                                   day30ChangePercent, day5ChangePercent, nextdividenddate, dividendyield, nextearningsdate, exdividenddate,
                                                   peratio, beta 
                                                  )
                          SELECT c.symbol, CAST(c.primarysiccode AS INT), c.securityname, c.companyname, c.issuetype, c.exchange, s.marketcap,
                            	 s.week52change, s.week52high, s.week52low,
                            	 CAST(s.sharesoutstanding AS BIGINT), s.maxChangePercent, s.year5ChangePercent, s.year2ChangePercent, s.year1ChangePercent,
                                 s.ytdChangePercent, s.month6ChangePercent, s.month3ChangePercent, s.month1ChangePercent,
                                 s.day30ChangePercent, s.day5ChangePercent, s.nextdividenddate, s.dividendyield, s.nextearningsdate, s.exdividenddate,
                            	 s.peratio, s.beta
                          FROM staging_companies c JOIN staging_stats s ON s.symbol = c.symbol
""")

company_dim_insert = ("""
                      INSERT INTO company_dim (company_name, industry, website, description, ceo, sector,
		                                      employees, address, address2, state, city,
		                                      zip, country, phone)
                      WITH paritioned_by_companies AS (
                		SELECT row_number() OVER (partition by companyname),
                			   companyname, industry, website, description, ceo, sector,
                			   CAST(employees AS BIGINT), address, address2, state, city,
                			   zip, country, phone
                		FROM staging_companies)
                     SELECT companyname, industry, website, description, ceo, sector,
                     		CAST(employees AS BIGINT), address, address2, state, city,
                     		zip, country, phone
                     FROM paritioned_by_companies
                     WHERE row_number = 1
""")

demographics_dim_insert = ("""INSERT INTO demographics_dim (city, state, median_age, male_population, female_population, total_population,
                                                           number_veterans, foreign_born, avg_household_size, state_code, race, count)
                              SELECT city, state, median_age, CAST(male_population AS INT), CAST(female_population AS INT), CAST(total_population AS INT),
                                     CAST(number_veterans AS INT), CAST(foreign_born AS INT), avg_household_size, state_code, race, CAST(count AS INT)
                              FROM staging_demographics
""")

time_dim_insert = ("""
                   INSERT INTO time_dim (date, day, week, month, year, weekday )
                   WITH partitioned_by_date AS (
                    	SELECT row_number() OVER (PARTITION BY date),
                    		   date,
                    		   EXTRACT(day FROM date)     AS day,
                    		   EXTRACT(week FROM date)    AS week,
                    		   EXTRACT(month FROM date)   AS month,
                    		   EXTRACT(year FROM date)    AS year,
                    		   EXTRACT(dow FROM date)     AS weekday
                    	FROM staging_daily_quotes)
                   SELECT date, day, week, month, year, weekday
                   FROM partitioned_by_date
                   WHERE row_number = 1
""")

# QUERY LISTS

create_table_queries = [staging_stats_create, staging_companies_create, staging_demographics_create,
                        staging_daily_quotes_create, time_dim_create, security_dim_create,
                        company_dim_create, demographics_dim_create, fact_table_create]
drop_table_queries = [staging_stats_drop, staging_companies_drop, staging_demographics_drop,
                      staging_daily_quotes_drop, time_dim_drop]

insert_queries = [security_dim_insert, company_dim_insert, demographics_dim_insert, time_dim_insert]

