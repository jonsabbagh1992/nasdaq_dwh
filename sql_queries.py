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

time_dim_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY,
                                                       day int NOT NULL,
                                                       week int NOT NULL,
                                                       month int NOT NULL,
                                                       year int NOT NULL,
                                                       weekday int NOT NULL);
""")

# Facts & Dims

security_dim_create = ("""CREATE TABLE IF NOT EXISTS securityDim (
                                    symbol VARCHAR PRIMARY KEY,
                                    securtiy_name VARCHAR NOT NULL,
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
company_dim_create = ("""CREATE TABLE IF NOT EXISTS companyDim (
                                    company_id SERIAL PRIMARY KEY,
                                    
                                )
                      
""")
security_dim_insert = ("""INSERT INTO securityDim (symbol, security_name, issue_type, exchange, market_cap BIGINT,
                                                   week52change FLOAT, week52high FLOAT, week52low FLOAT,
                                                   shares_outstanding BIGINT,
                                                   maxChangePercent FLOAT, year5ChangePercent FLOAT, year2ChangePercent FLOAT, year1ChangePercent FLOAT,
                                                   ytdChangePercent FLOAT, month6ChangePercent FLOAT, month3ChangePercent FLOAT, month1ChangePercent FLOAT,
                                                   day30ChangePercent FLOAT, day5ChangePercent FLOAT, nextdividenddate DATE, dividendyield FLOAT, nextearningsdate DATE, exdividenddate DATE,
                                                   peratio FLOAT, beta FLOAT 
                                                  )
                          SELECT c.symbol, c.securityname, c.issuetype, c.exchange, s.marketcap,
                            	 s.week52change, s.week52high, s.week52low,
                            	 CAST(s.sharesoutstanding AS BIGINT), s.maxChangePercent, s.year5ChangePercent, s.year2ChangePercent, s.year1ChangePercent,
                                 s.ytdChangePercent, s.month6ChangePercent, s.month3ChangePercent, s.month1ChangePercent,
                                 s.day30ChangePercent, s.day5ChangePercent, s.nextdividenddate, s.dividendyield, s.nextearningsdate, s.exdividenddate,
                            	 s.peratio, s.beta
                          FROM staging_companies c JOIN staging_stats s ON s.symbol = c.symbol
""")

# QUERY LISTS

create_table_queries = [staging_stats_create, staging_companies_create, staging_demographics_create,
                        staging_daily_quotes_create, time_dim_create, security_dim_create]
drop_table_queries = [staging_stats_drop, staging_companies_drop, staging_demographics_drop,
                      staging_daily_quotes_drop, time_dim_drop]