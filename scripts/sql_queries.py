# DROP TABLES

staging_stats_drop = "DROP TABLE IF EXISTS staging.stats"
staging_companies_drop = "DROP TABLE IF EXISTS staging.companies"
staging_demographics_drop = "DROP TABLE IF EXISTS staging.demographics"
staging_daily_quotes_drop = "DROP TABLE IF EXISTS staging.daily_quotes"
time_dim_drop = "DROP TABLE IF EXISTS time_dim CASCADE"
security_dim_drop = "DROP TABLE IF EXISTS security_dim CASCADE"
company_dim_drop = "DROP TABLE IF EXISTS company_dim CASCADE"
demographics_dim_drop = "DROP TABLE IF EXISTS demographics_dim CASCADE"
facts_drop = "DROP TABLE IF EXISTS daily_quotes_fact CASCADE"



# CREATE TABLES

# Staging
staging_schema_create = "CREATE SCHEMA IF NOT EXISTS staging"

staging_stats_create = ("""CREATE TABLE IF NOT EXISTS staging.stats (
                                     symbol VARCHAR,
                                     week52change FLOAT, week52high FLOAT, week52low FLOAT, marketcap FLOAT, employees FLOAT,
                                     day200MovingAvg FLOAT, day50MovingAvg FLOAT, float FLOAT, avg10Volume FLOAT, avg30Volume FLOAT,
                                     ttmEPS FLOAT, ttmDividendRate FLOAT, companyName VARCHAR, sharesOutstanding FLOAT,
                                     maxChangePercent FLOAT, year5ChangePercent FLOAT, year2ChangePercent FLOAT, year1ChangePercent FLOAT,
                                     ytdChangePercent FLOAT, month6ChangePercent FLOAT, month3ChangePercent FLOAT, month1ChangePercent FLOAT,
                                     day30ChangePercent FLOAT, day5ChangePercent FLOAT, nextdividenddate DATE, dividendyield FLOAT, nextearningsdate DATE, exdividenddate DATE,
                                     peratio FLOAT, beta FLOAT);
""")

staging_companies_create = ("""CREATE TABLE IF NOT EXISTS staging.companies (
                                    symbol VARCHAR, companyName VARCHAR, exchange VARCHAR, industry VARCHAR,
                                    website VARCHAR, description VARCHAR, CEO VARCHAR, securityName VARCHAR, issueType VARCHAR,
                                    sector VARCHAR, primarySicCode FLOAT, employees FLOAT, address VARCHAR, address2 VARCHAR,
                                    state VARCHAR, city VARCHAR, zip VARCHAR, country VARCHAR, phone VARCHAR);
""")

staging_demographics_create = ("""CREATE TABLE IF NOT EXISTS staging.demographics (
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

staging_daily_quotes_create = ("""CREATE TABLE IF NOT EXISTS staging.daily_quotes(
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
                                    security_id SERIAL PRIMARY KEY,
                                    symbol VARCHAR NOT NULL,
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

fact_table_create_with_referential = ("""CREATE TABLE IF NOT EXISTS daily_quotes_fact (
                                    quote_id SERIAL PRIMARY KEY,
                                    security_id int NOT NULL REFERENCES security_dim (security_id),
                                    company_id INT NOT NULL REFERENCES company_dim (company_id),
                                    date DATE NOT NULL REFERENCES time_dim (date),
                                    demographic_id INT NOT NULL REFERENCES demographics_dim (demographic_id),
                                    open FLOAT,
                                    high FLOAT,
                                    low FLOAT,
                                    close FLOAT,
                                    adj_close FLOAT,
                                    volume BIGINT
                        )
                     
""")

fact_table_create_no_referential = ("""CREATE TABLE IF NOT EXISTS daily_quotes_fact (
                                    quote_id SERIAL PRIMARY KEY,
                                    security_id int,
                                    company_id INT,
                                    date DATE,
                                    demographic_id INT,
                                    open FLOAT,
                                    high FLOAT,
                                    low FLOAT,
                                    close FLOAT,
                                    adj_close FLOAT,
                                    volume BIGINT
                        )
                     
""")

#CREATE INDICES
symbol_fact_index  = ("CREATE INDEX security_fact_index ON daily_quotes_fact(security_id)")
company_fact_index  = ("CREATE INDEX company_fact_index ON daily_quotes_fact(company_id)")
demographic_fact_index  = ("CREATE INDEX demographic_fact_index ON daily_quotes_fact(demographic_id)")


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
                          FROM staging.companies c JOIN staging.stats s ON s.symbol = c.symbol
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
                		FROM staging.companies)
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
                              FROM staging.demographics
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
                    	FROM staging.daily_quotes)
                   SELECT date, day, week, month, year, weekday
                   FROM partitioned_by_date
                   WHERE row_number = 1
""")

quotes_fact_insert = ("""WITH first_join AS (
                        	SELECT s.security_id, s.symbol, c.company_id, c.city, c.state
                        	FROM company_dim c JOIN security_dim s ON s.company_name = c.company_name
                        ),
                        second_join AS (
                        	SELECT row_number() OVER(PARTITION BY symbol), f.symbol, f.security_id, f.company_id, d.demographic_id
                        	FROM first_join f JOIN demographics_dim d ON d.city = f.city AND d.state = f.state
                        ),
                        unique_ids AS (
                        	SELECT * FROM second_join
                        	WHERE row_number = 1
                        )
						INSERT INTO daily_quotes_fact (security_id, company_id, date, demographic_id, open, high, low, close, adj_close, volume)
                        SELECT s.security_id, s.company_id, q.date, s.demographic_id, q.open, q.high, q.low, q.close, q.adj_close, CAST(q.volume AS BIGINT)
                        FROM staging.daily_quotes q JOIN unique_ids s ON q.symbol = s.symbol
        """)
                        
# DATA QUALITY QUERIES
check_company_count = 'SELECT count(*) FROM company_dim'
check_security_count = 'SELECT count(*) FROM security_dim'
check_demographic_count = 'SELECT count(*) FROM demographics_dim'
check_time_count = 'SELECT count(*) FROM time_dim'

# QUERY COLLECTIONS

create_table_queries_no_referential = [staging_schema_create, staging_stats_create, staging_companies_create, staging_demographics_create,
                        staging_daily_quotes_create, time_dim_create, security_dim_create,
                        company_dim_create, demographics_dim_create, fact_table_create_no_referential]

create_table_queries_with_referential = [staging_schema_create, staging_stats_create, staging_companies_create, staging_demographics_create,
                        staging_daily_quotes_create, time_dim_create, security_dim_create,
                        company_dim_create, demographics_dim_create, fact_table_create_with_referential]


drop_table_queries = [staging_stats_drop, staging_companies_drop, staging_demographics_drop,
                      staging_daily_quotes_drop, time_dim_drop, security_dim_drop, company_dim_drop, demographics_dim_drop, facts_drop]

index_queries = [symbol_fact_index, company_fact_index, demographic_fact_index]

insert_queries = [security_dim_insert, company_dim_insert, demographics_dim_insert, time_dim_insert, quotes_fact_insert]

data_quality_checks = {
        check_company_count: 7680,
        check_security_count: 8042,
        check_demographic_count: 2891,
        check_facts_null: 0,
        check_time_count: 14718
        }

