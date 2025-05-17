# StrataScratch - Technical Questions (only the "Free" ones)
# **********************************************************
# https://platform.stratascratch.com/coding?code_type=2&is_freemium=1

# My own solutions - both in Python Pandas and in MySQL

# ***** In progress *****


# Contents

# 1. Difficulty: Easy  (27 Questions)
#    1.1 Unique Users per Client per Month
#    1.2 Number of Shipments per Month
#    1.3 Most Lucrative Products
#    1.4 Number of Bathrooms and Bedrooms
#    1.5 MacBookPro User Event Count
#    1.6 Most Profitable Financial Company
#    1.7 Churro Activity Date
#    1.8 Number of violations
#    1.9 April Admin Employees
#    1.10 Workers by Department Since April
#    1.11 Highly Reviewed Hotels
#    1.12 Customer Details
#    1.13 Order Details
#    1.14 Average Salaries
#    1.15 Email Preference Missing
#    1.16 Captain Base Pay
#    1.17 Top Ranked Songs
#    1.18 Artist Appearance Count
#    1.19 Lyft Driver Wages
#    1.20 Popularity of Hack
#    1.21 Find all posts which were reacted to with a heart
#    1.22 Abigail Breslin Nominations
#    1.23 Reviews of Hotel Arena
#    1.24 Bikes Last Used
#    1.25 Finding Updated Records
#    1.26 Salaries Differences
#    1.27 Workers With The Highest Salaries

# 2. Difficulty: Medium  (41 Questions)
#    2.1 Share of Active Users
#    2.2 Premium Accounts
#    2.3 Election Results
#    2.4 Flags per Video
#    2.5 User with Most Approved Flags
#    2.6 Find Students At Median Writing
#    2.7 Top 10 Songs 2010
#    2.8 Classify Business Type
#    2.9 ***** In progress *****
#    2.10 
#    2.11 
#    2.12 

# 3. Difficulty: Hard  (12 Questions)
#    3.1 ***** In progress *****
#    3.2 
#    3.3 
#    3.4 



# 1. Difficulty: Easy  (27 Questions)
# ***********************************

# 1.1 Unique Users per Client per Month
# https://platform.stratascratch.com/coding/2024-unique-users-per-client-per-month?code_type=2

# Write a query that returns the number of unique users per client per month.


# Python
# ******
import pandas as pd

fact_events = fact_events.assign(date_month = fact_events["time_id"].dt.month)

df_f_e_gr = fact_events.groupby(by=["client_id", "date_month"], as_index=False)["user_id"].nunique()
df_f_e_gr = df_f_e_gr.rename(columns={"user_id": "users_count"})
df_f_e_gr


# MySQL
# *****
SELECT
    client_id,
    EXTRACT(MONTH FROM time_id) AS date_month,
    COUNT(DISTINCT user_id) AS users_count
FROM fact_events
GROUP BY client_id, date_month;



# 1.2 Number of Shipments per Month
# https://platform.stratascratch.com/coding/2056-number-of-shipments-per-month?code_type=2

# Write a query that will calculate the number of shipments per month.
# The unique key for one shipment is a combination of shipment_id and sub_id.
# Output the year_month in format YYYY-MM and the number of shipments in that month.


# Python
# ******
import pandas as pd

amazon_shipment = amazon_shipment.assign(date_year_month = amazon_shipment["shipment_date"].dt.strftime("%Y-%m"))
df_a_s_gr = amazon_shipment.groupby(by="date_year_month", as_index=False)["shipment_id"].count()
df_a_s_gr = df_a_s_gr.rename(columns={"shipment_id": "shipments_count"})
df_a_s_gr


# MySQL
# *****
SELECT
    DATE_FORMAT(shipment_date, '%Y-%m') AS date_year_month,
    COUNT(shipment_id) AS shipments_count
FROM amazon_shipment
GROUP BY date_year_month;



# 1.3 Most Lucrative Products
# https://platform.stratascratch.com/coding/2119-most-lucrative-products?code_type=2

# Find the 5 most lucrative products in terms of total revenue for the first half of 2022 (from January to June inclusive).
# Output their IDs and the total revenue.


# Python
# ******
import pandas as pd

online_orders = online_orders.assign(revenue = online_orders["cost_in_dollars"] * online_orders["units_sold"])

df_o_o_fltr = online_orders[online_orders["date_sold"].dt.month.between(1, 6, inclusive="both")]

df_o_o_gr = df_o_o_fltr.groupby(by="product_id", as_index=False)["revenue"].sum().nlargest(5, "revenue")


# MySQL
# *****
SELECT
    product_id,
    SUM(cost_in_dollars * units_sold) AS revenue
FROM online_orders
WHERE MONTH(date_sold) BETWEEN 1 AND 6
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 5;



# 1.4 Number of Bathrooms and Bedrooms
# https://platform.stratascratch.com/coding/9622-number-of-bathrooms-and-bedrooms?code_type=2

# Find the average number of bathrooms and bedrooms for each city’s property types.
# Output the result along with the city name and the property type.


# Python
# ******
import pandas as pd

df_grp_agg = airbnb_search_details.groupby(by=["city", "property_type"], as_index=False).agg({"bathrooms": "mean", "bedrooms": "mean"})
df_grp_agg = df_grp_agg.rename(columns={"bathrooms": "n_bathrooms_avg", "bedrooms": "n_bedrooms_avg"})
df_grp_agg


# MySQL
# *****
SELECT
    city,
    property_type,
    AVG(bathrooms) AS n_bathrooms_avg,
    AVG(bedrooms)  AS n_bedrooms_avg
FROM airbnb_search_details
GROUP BY city, property_type
ORDER BY city, property_type;



# 1.5 MacBookPro User Event Count
# https://platform.stratascratch.com/coding/9653-count-the-number-of-user-events-performed-by-macbookpro-users?code_type=2

# Count the number of user events performed by MacBookPro users.
# Output the result along with the event name.
# Sort the result based on the event count in the descending order.


# Python
# ******
import pandas as pd

df_p_e_gr = playbook_events[playbook_events["device"].eq("macbook pro")].groupby(by="event_name").size().to_frame("event_count").reset_index()
df_p_e_gr.sort_values(by="event_count", ascending=False)


# MySQL
# *****
SELECT
    event_name,
    COUNT(*) AS event_count
FROM playbook_events
WHERE device = 'macbook pro'
GROUP BY event_name
ORDER BY event_count DESC;



# 1.6 Most Profitable Financial Company
# https://platform.stratascratch.com/coding/9663-find-the-most-profitable-company-in-the-financial-sector-of-the-entire-world-along-with-its-continent?code_type=2

# Find the most profitable company from the financial sector.
# Output the result along with the continent.


# Python
# ******
import pandas as pd

forbes_global_2010_2014[forbes_global_2010_2014["sector"].eq("Financials")].nlargest(1, "profits")[["company", "continent"]]


# MySQL
# *****
SELECT
    company,
    continent
FROM forbes_global_2010_2014
WHERE sector = 'Financials'
ORDER BY profits DESC
LIMIT 1;



# 1.7 Churro Activity Date
# https://platform.stratascratch.com/coding/9688-churro-activity-date?code_type=2

# Find the inspection date and risk category (pe_description) of facilities named 'STREET CHURROS' that received a score below 95.


# Python
# ******
import pandas as pd

df = los_angeles_restaurant_health_inspections

df[
    (df["facility_name"].eq("STREET CHURROS")) &
    (df["score"].lt(95))
][["activity_date", "pe_description"]]



# MySQL
# *****
SELECT
    activity_date,
    pe_description
FROM los_angeles_restaurant_health_inspections
WHERE
    facility_name = 'STREET CHURROS' AND
    score < 95;



# 1.8 Number of violations
# https://platform.stratascratch.com/coding/9728-inspections-that-resulted-in-violations?code_type=2

# You are given a dataset of health inspections that includes details about violations.
# Each row represents an inspection, and if an inspection resulted in a violation, the violation_id column will contain a value.
# Count the total number of violations that occurred at 'Roxanne Cafe' for each year, based on the inspection date.
# Output the year and the corresponding number of violations in ascending order of the year.


# Python
# ******
import pandas as pd

df = sf_restaurant_health_violations[sf_restaurant_health_violations["business_name"].eq("Roxanne Cafe")]
df = df.assign(insp_year = df["inspection_date"].dt.year)

df_gr = df.groupby(by="insp_year", as_index=False)["violation_id"].count()
df_gr = df_gr.rename(columns={"violation_id": "violations_count"})


# MySQL
# *****
SELECT
    YEAR(inspection_date) AS insp_year,
    COUNT(violation_id) AS violations_count
FROM sf_restaurant_health_violations
WHERE business_name = 'Roxanne Cafe'
GROUP BY insp_year
ORDER BY insp_year;



# 1.9 April Admin Employees
# https://platform.stratascratch.com/coding/9845-find-the-number-of-employees-working-in-the-admin-department?code_type=2

# Find the number of employees working in the Admin department that joined in April or later.


# Python
# ******
import pandas as pd

admin_empl_count = worker[worker["department"].eq("Admin") & worker["joining_date"].dt.month.ge(4)]["worker_id"].count()


# MySQL
# *****
SELECT
    COUNT(*) AS admin_empl_count
FROM worker
WHERE
    department = 'Admin' AND
    MONTH(joining_date) >= 4;



# 1.10 Workers by Department Since April
# https://platform.stratascratch.com/coding/9847-find-the-number-of-workers-by-department?code_type=2

# Find the number of workers by department who joined on or after April 1, 2014.
# Output the department name along with the corresponding number of workers.
# Sort the results based on the number of workers in descending order.


# Python
# ******
import pandas as pd

df_gr = worker[worker["joining_date"].ge("2014-04-01")].groupby(by="department", as_index=False)["worker_id"].count()
df_gr = df_gr.rename(columns={"worker_id": "workers_count"})
df_gr.sort_values(by="workers_count", ascending=False)


# MySQL
# *****
SELECT
    department,
    COUNT(*) AS workers_count
FROM worker
WHERE joining_date >= '2014-04-01'
GROUP BY department
ORDER BY workers_count DESC;



# 1.11 Highly Reviewed Hotels
# https://platform.stratascratch.com/coding/9871-highly-reviewed-hotels?code_type=2

# List all hotels along with their total number of reviews using the total_number_of_reviews column.
# Sort the results by total reviews in descending order.


# Python
# ******
import pandas as pd

hotel_reviews[["hotel_name", "total_number_of_reviews"]].sort_values(by="total_number_of_reviews", ascending=False).drop_duplicates()


# MySQL
# *****
SELECT DISTINCT
    hotel_name,
    total_number_of_reviews
FROM hotel_reviews
ORDER BY total_number_of_reviews DESC;



# 1.12 Customer Details
# https://platform.stratascratch.com/coding/9891-customer-details?code_type=2

# Find the details of each customer regardless of whether the customer made an order.
# Output the customer's first name, last name, and the city along with the order details.
# Sort records based on the customer's first name and the order details in ascending order.


# Python
# ******
import pandas as pd

customers.merge(orders, left_on="id", right_on="cust_id", how="left")[["first_name", "last_name", "city", "order_details"]].sort_values(by=["first_name", "order_details"])


# MySQL
# *****
SELECT
    first_name,
    last_name,
    city,
    order_details
FROM customers
LEFT JOIN orders
    ON customers.id = orders.cust_id
ORDER BY first_name, order_details;



# 1.13 Order Details
# https://platform.stratascratch.com/coding/9913-order-details?code_type=2

# Find order details made by Jill and Eva. Consider the Jill and Eva as first names of customers.
# Output the order date, details and cost along with the first name.
# Order records based on the customer id in ascending order.


# Python
# ******
import pandas as pd

cust_list = ["Jill", "Eva"]

customers[
    customers["first_name"].isin(cust_list)
].merge(orders, left_on="id", right_on="cust_id").sort_values(by="cust_id")[["first_name", "order_date", "order_details", "total_order_cost"]]


# MySQL
# *****
SELECT
    first_name,
    order_date,
    order_details,
    total_order_cost
FROM customers
JOIN orders
    ON customers.id = orders.cust_id
WHERE first_name IN ('Jill', 'Eva')
ORDER BY cust_id;



# 1.14 Average Salaries
# https://platform.stratascratch.com/coding/9917-average-salaries?code_type=2

# Compare each employee's salary with the average salary of the corresponding department.
# Output the department, first name, and salary of employees along with the average salary of that department.


# Python
# ******
# Solution #1 - with added new column "avg_salary" using groupby(...)[...].transform("mean")
import pandas as pd

employee = employee.assign(avg_salary = employee.groupby(by="department")["salary"].transform("mean"))
employee[["department", "first_name", "salary", "avg_salary"]].sort_values(by="department")


# Solution #2 - using a new DataFrame with calculated average salaries by department and then .merge(...)
import pandas as pd

df_dpt_avg_sal = employee.groupby(by="department", as_index=False)["salary"].mean()
df_dpt_avg_sal = df_dpt_avg_sal.rename(columns={"salary": "avg_salary"})

df_dpt_avg_sal.merge(employee, on="department")[["department", "first_name", "salary", "avg_salary"]]


# MySQL
# *****
# Solution #1 - using a Window Function OVER(PARTITION BY ...)
SELECT
    department,
    first_name,
    salary,
    AVG(salary) OVER(PARTITION BY department) AS avg_salary
FROM employee
ORDER BY department;


# Solution #2 - using a CTE with calculated average salaries by department and then JOIN
WITH cte_dpt_avg_sal AS
(
SELECT
    department,
    AVG(salary) AS avg_salary
FROM employee
GROUP BY department
ORDER BY department
)
SELECT
    cdas.department,
    first_name,
    salary,
    avg_salary
FROM cte_dpt_avg_sal AS cdas
JOIN employee AS emp
    ON cdas.department = emp.department
ORDER BY department;



# 1.15 Email Preference Missing
# https://platform.stratascratch.com/coding/9924-find-libraries-who-havent-provided-the-email-address-in-2016-but-their-notice-preference-definition-is-set-to-email?code_type=2

# Find libraries from the 2016 circulation year that have no email address provided but have their notice preference set to email.
# In your solution, output their home library code.


# Python
# ******
import pandas as pd

library_usage[
    library_usage["circulation_active_year"].eq(2016) &
    library_usage["notice_preference_definition"].eq("email") &
    library_usage["provided_email_address"].eq(False)
][["home_library_code"]].drop_duplicates()


# MySQL
# *****
SELECT DISTINCT
    home_library_code
FROM library_usage
WHERE
    circulation_active_year = 2016 AND
    notice_preference_definition = 'email' AND
    provided_email_address = FALSE;



# 1.16 Captain Base Pay
# https://platform.stratascratch.com/coding/9972-find-the-base-pay-for-police-captains?code_type=2

# Find the base pay for Police Captains.
# Output the employee name along with the corresponding base pay.


# Python
# ******
import pandas as pd

sf_public_salaries[
    sf_public_salaries["jobtitle"].str.contains("captain", case=False, regex=False) &
    sf_public_salaries["jobtitle"].str.contains("police" , case=False, regex=False)
][["employeename", "basepay"]]


# MySQL
# *****
SELECT
    employeename,
    basepay
FROM sf_public_salaries
WHERE
    jobtitle LIKE '%captain%' AND
    jobtitle LIKE '%police%';



# 1.17 Top Ranked Songs
# https://platform.stratascratch.com/coding/9991-top-ranked-songs?code_type=2

# Find songs that have ranked in the top position. Output the track name and the number of times it ranked at the top.
# Sort your records by the number of times the song was in the top position in descending order.


# Python
# ******
# Solution #1 - using .groupby(...) .count() and then rename the column
import pandas as pd

df_gr = spotify_worldwide_daily_song_ranking[
    spotify_worldwide_daily_song_ranking["position"].eq(1)
].groupby(by="trackname", as_index=False)["position"].count()

df_gr = df_gr.rename(columns={"position": "times_top1"})
df_gr.sort_values(by="times_top1", ascending=False)


# Solution #2 - using .groupby(...) .size().to_frame(...).reset_index()
import pandas as pd

df_gr = spotify_worldwide_daily_song_ranking[
    spotify_worldwide_daily_song_ranking["position"].eq(1)
].groupby(by="trackname").size().to_frame("times_top1").reset_index().sort_values(by="times_top1", ascending=False)


# MySQL
# *****
SELECT
    trackname,
    COUNT(*) AS times_top1
FROM spotify_worldwide_daily_song_ranking
WHERE position = 1
GROUP BY trackname
ORDER BY times_top1 DESC;



# 1.18 Artist Appearance Count
# https://platform.stratascratch.com/coding/9992-find-artists-that-have-been-on-spotify-the-most-number-of-times?code_type=2

# Find how many times each artist appeared on the Spotify ranking list.
# Output the artist name along with the corresponding number of occurrences.
# Order records by the number of occurrences in descending order.


# Python
# ******
# Solution #1 - using .groupby(...) .count() and then rename the column
import pandas as pd

df_gr = spotify_worldwide_daily_song_ranking.groupby(by="artist", as_index=False)["position"].count()
df_gr = df_gr.rename(columns={"position": "n_occurences"})
df_gr.sort_values(by="n_occurences", ascending=False)


# Solution #2 - using .groupby(...) .size().to_frame(...).reset_index()
import pandas as pd

df_gr = spotify_worldwide_daily_song_ranking.groupby(by="artist").size().to_frame("n_occurences").reset_index().sort_values(by="n_occurences", ascending=False)


# MySQL
# *****
SELECT
    artist,
    COUNT(*) AS n_occurences
FROM spotify_worldwide_daily_song_ranking
GROUP BY artist
ORDER BY n_occurences DESC;



# 1.19 Lyft Driver Wages
# https://platform.stratascratch.com/coding/10003-lyft-driver-wages?code_type=2

# Find all Lyft drivers who earn either equal to or less than 30k USD or equal to or more than 70k USD.
# Output all details related to retrieved records.


# Python
# ******
import pandas as pd

lyft_drivers[
    lyft_drivers["yearly_salary"].le(30000) |
    lyft_drivers["yearly_salary"].ge(70000)
]


# MySQL
# *****
SELECT *
FROM lyft_drivers
WHERE
    yearly_salary <= 30000 OR
    yearly_salary >= 70000;



# 1.20 Popularity of Hack
# https://platform.stratascratch.com/coding/10061-popularity-of-hack?code_type=2

# Find the average popularity of the 'Hack' programming language per office location.
# Output the location along with the average popularity.


# Python
# ******
import pandas as pd

df = facebook_hack_survey.merge(facebook_employees, left_on="employee_id", right_on="id").groupby(by="location", as_index=False)["popularity"].mean()
df = df.rename(columns={"popularity": "avg_popularity"})


# MySQL
# *****
SELECT
    location,
    AVG(popularity) AS avg_popularity
FROM facebook_hack_survey AS hs
JOIN facebook_employees AS em
    ON hs.employee_id = em.id
GROUP BY location;



# 1.21 Find all posts which were reacted to with a heart
# https://platform.stratascratch.com/coding/10087-find-all-posts-which-were-reacted-to-with-a-heart?code_type=2

# Find all posts which were reacted to with a heart.
# For such posts output all columns from facebook_posts table.


# Python
# ******
import pandas as pd

facebook_reactions[facebook_reactions["reaction"].eq("heart")][["post_id"]].drop_duplicates().merge(facebook_posts, on="post_id")


# MySQL
# *****
# Solution #1
SELECT DISTINCT
    p.*
FROM facebook_posts AS p
JOIN facebook_reactions AS r
    ON p.post_id = r.post_id AND
    reaction = 'heart';


# Solution #2 - using a CTE
WITH cte_heart AS
(
SELECT DISTINCT
    post_id
FROM facebook_reactions
WHERE reaction = 'heart'
)
SELECT p.*
FROM facebook_posts AS p
JOIN cte_heart AS h
    ON p.post_id = h.post_id;



# 1.22 Abigail Breslin Nominations
# https://platform.stratascratch.com/coding/10128-count-the-number-of-movies-that-abigail-breslin-nominated-for-oscar?code_type=2

# Count the number of movies for which Abigail Breslin was nominated for an Oscar.


# Python
# ******
import pandas as pd

oscar_nominees[oscar_nominees["nominee"].eq("Abigail Breslin")]["movie"].nunique()


# MySQL
# *****
SELECT
    COUNT(DISTINCT movie)
FROM oscar_nominees
WHERE nominee = 'Abigail Breslin';



# 1.23 Reviews of Hotel Arena
# https://platform.stratascratch.com/coding/10166-reviews-of-hotel-arena?code_type=2

# Find how many reviews exist for each review score given to 'Hotel Arena'.
# Output the hotel name ('Hotel Arena'), each review score, and the number of reviews for that score.


# Python
# ******
import pandas as pd

hotel_reviews[hotel_reviews["hotel_name"].eq("Hotel Arena")].groupby(["hotel_name", "reviewer_score"]).size().to_frame("rev_count").reset_index()


# MySQL
# *****
SELECT
    hotel_name,
    reviewer_score,
    COUNT(*) AS rev_count
FROM hotel_reviews
WHERE hotel_name = 'Hotel Arena'
GROUP BY reviewer_score
ORDER BY reviewer_score;



# 1.24 Bikes Last Used
# https://platform.stratascratch.com/coding/10176-bikes-last-used?code_type=2

# Find the last time each bike was in use.
# Output both the bike number and the date-timestamp of the bike's last use (i.e., the date-time the bike was returned).
# Order the results by bikes that were most recently used.


# Python
# ******
# Solution #1 - using .groupby(...) .max() and then rename the column
import pandas as pd

df = dc_bikeshare_q1_2012.groupby(by="bike_number", as_index=False)["end_time"].max()
df = df.rename(columns={"end_time": "last_used"})
df.sort_values(by="last_used", ascending=False)


# Solution #2 - using .groupby(...) .max().to_frame(...).reset_index()
import pandas as pd

dc_bikeshare_q1_2012.groupby(by="bike_number")["end_time"].max().to_frame("last_used").reset_index().sort_values(by="last_used", ascending=False)


# MySQL
# *****
# Solution #1 - using GROUP BY
SELECT
    bike_number,
    MAX(end_time) AS last_used
FROM dc_bikeshare_q1_2012
GROUP BY bike_number
ORDER BY last_used DESC;


# Solution #2 - using SELECT DISTINCT and a Window Function OVER(PARTITION BY ...)
SELECT DISTINCT
    bike_number,
    MAX(end_time) OVER(PARTITION BY bike_number) AS last_used
FROM dc_bikeshare_q1_2012
ORDER BY last_used DESC;



# 1.25 Finding Updated Records
# https://platform.stratascratch.com/coding/10299-finding-updated-records?code_type=2

# We have a table with employees and their salaries, however, some of the records are old and contain outdated salary information.
# Find the current salary of each employee assuming that salaries increase each year.
# Output their id, first name, last name, department ID, and current salary.
# Order your list by employee ID in ascending order.


# Python
# ******
import pandas as pd

ms_employee_salary.groupby(by=["id", "first_name", "last_name", "department_id"])["salary"].max().to_frame("curr_salary").reset_index().sort_values(by="id")


# MySQL
# *****
SELECT
    id,
    first_name,
    last_name,
    department_id,
    MAX(salary) AS curr_salary
FROM ms_employee_salary
GROUP BY id
ORDER BY id;



# 1.26 Salaries Differences
# https://platform.stratascratch.com/coding/10308-salaries-differences?code_type=2

# Calculates the difference between the highest salaries in the marketing and engineering departments.
# Output just the absolute difference in salaries.


# Python
# ******
import pandas as pd

salary_diff = abs( db_employee[db_employee["department_id"].eq(1)]["salary"].max() - db_employee[db_employee["department_id"].eq(4)]["salary"].max() )


# MySQL
# *****
SELECT
    ABS(
    (SELECT MAX(salary) FROM db_employee WHERE department_id = 1) - 
    (SELECT MAX(salary) FROM db_employee WHERE department_id = 4)
    ) AS salary_diff;



# 1.27 Workers With The Highest Salaries
# https://platform.stratascratch.com/coding/10353-workers-with-the-highest-salaries?code_type=2

# Find the job titles of the employees with the highest salary.
# If multiple employees have the same highest salary, include the job titles for all such employees.


# Python
# ******
import pandas as pd

worker[worker["salary"].eq(worker["salary"].max())].merge(title, left_on="worker_id", right_on="worker_ref_id")["worker_title"]


# MySQL
# *****
SELECT
    worker_title
FROM worker AS w
JOIN title AS t
    ON w.worker_id = t.worker_ref_id
WHERE salary = (SELECT MAX(salary) FROM worker);





# 2. Difficulty: Medium  (41 Questions)
# *************************************

# 2.1 Share of Active Users
# https://platform.stratascratch.com/coding/2005-share-of-active-users?code_type=2

# Calculate the percentage of users who are both from the US and have an 'open' status, as indicated in the fb_active_users table.


# Python
# ******
import pandas as pd

usa_active_pctg = fb_active_users[
    fb_active_users["country"].eq("USA") &
    fb_active_users["status"].eq("open")
].shape[0] / fb_active_users.shape[0] * 100


# MySQL
# *****
SELECT
    COUNT(*) / (SELECT COUNT(*) FROM fb_active_users) * 100 AS usa_active_pctg
FROM fb_active_users
WHERE
    country = 'USA' AND
    status  = 'open';



# 2.2 Premium Accounts
# https://platform.stratascratch.com/coding/2097-premium-acounts?code_type=2

# You have a dataset that records daily active users for each premium account.
# A premium account appears in the data every day as long as it remains premium.
# However, some premium accounts may be temporarily discounted, meaning they are not actively paying — this is indicated by a final_price of 0.
# For each of the first 7 available dates, count the number of premium accounts that were actively paying on that day.
# Then, track how many of those same accounts remain premium and are still paying exactly 7 days later (regardless of activity in between).
# Output three columns:
# - The date of initial calculation.
# - The number of premium accounts that were actively paying on that day.
# - The number of those accounts that remain premium and are still paying after 7 days.


# Python
# ******
import pandas as pd

df = premium_accounts_by_day.assign(date_p_7d = premium_accounts_by_day["entry_date"] + pd.DateOffset(7))

dfm = df.merge(df, how="left", left_on=["account_id", "date_p_7d"], right_on=["account_id", "entry_date"], suffixes=("_L", "_R"))
dfm = dfm[dfm["final_price_L"].gt(0)][["account_id", "entry_date_L", "final_price_L", "final_price_R"]]
dfm = dfm.rename(columns={"entry_date_L": "entry_date"})

dfm_gr = dfm.groupby(by="entry_date", as_index=False).agg(
    premium_paid_acc_count         =("final_price_L", "count"),
    premium_paid_acc_count_after_7d=("final_price_R", lambda x: x.gt(0).sum())
    ).head(7)


# MySQL
# *****
SELECT
    p1.entry_date,
    COUNT(p1.final_price)            AS premium_paid_acc_count,
    COUNT(NULLIF(p2.final_price, 0)) AS premium_paid_acc_count_after_7d
FROM premium_accounts_by_day AS p1
LEFT JOIN premium_accounts_by_day AS p2
    ON p1.account_id = p2.account_id AND
    ADDDATE(p1.entry_date, 7) = p2.entry_date
WHERE
    p1.final_price > 0
GROUP BY p1.entry_date
LIMIT 7;



# 2.3 Election Results
# https://platform.stratascratch.com/coding/2099-election-results?code_type=2

# The election is conducted in a city and everyone can vote for one or more candidates, or choose not to vote at all.
# Each person has 1 vote so if they vote for multiple candidates, their vote gets equally split across these candidates.
# For example, if a person votes for 2 candidates, these candidates receive an equivalent of 0.5 vote each.
# Some voters have chosen not to vote, which explains the blank entries in the dataset.
# Find out who got the most votes and won the election. Output the name of the candidate, or multiple names in case of a tie.
# To avoid issues with a floating-point error you can round the number of votes received by a candidate to 3 decimal places.


# Python
# ******
import pandas as pd

df = voting_results[
    voting_results["candidate"].notna() & 
    voting_results["candidate"].str.strip().ne("")
    ].sort_values(by=["voter", "candidate"]).reset_index(drop=True)

df = df.assign(vote_value = 1 / df.groupby(by="voter")["candidate"].transform("count"))

df_results = df.groupby(by="candidate", as_index=False)["vote_value"].sum().round(3)
df_results = df_results.rename(columns={"vote_value": "vote_value_sum"})

df_results = df_results.assign(vote_rank = df_results["vote_value_sum"].rank(method="dense", ascending=False))
df_results["vote_rank"] = df_results["vote_rank"].astype(int, errors="ignore")              # This line is not required, but nice for presentation of the overall results
df_results = df_results.sort_values(by=["vote_rank", "candidate"]).reset_index(drop=True)   # This line is not required, but nice for presentation of the overall results
df_results[df_results["vote_rank"].eq(1)]["candidate"]


# MySQL
# *****
WITH cte_vote_value AS
(
SELECT
    voter,
    candidate,
    1 / COUNT(*) OVER(PARTITION BY voter) AS vote_value
FROM voting_results
WHERE
    TRIM(candidate) != '' AND
    candidate IS NOT NULL
ORDER BY voter, candidate
),
cte_vote_value_sum AS
(
SELECT
    candidate,
    ROUND(SUM(vote_value), 3) AS vote_value_sum
FROM cte_vote_value
GROUP BY candidate
),
cte_rank AS
(
SELECT
    *,
    DENSE_RANK() OVER(ORDER BY vote_value_sum DESC) AS vote_rank
FROM cte_vote_value_sum
)
SELECT candidate
FROM cte_rank
WHERE vote_rank = 1;



# 2.4 Flags per Video
# https://platform.stratascratch.com/coding/2102-flags-per-video?code_type=2

# For each video, find how many unique users flagged it.
# A unique user can be identified using the combination of their first name and last name.
# Do not consider rows in which there is no flag ID.


# Python
# ******
import pandas as pd

df = user_flags[user_flags["flag_id"].notna() & user_flags["flag_id"].str.strip().ne("")]
df = df.assign(user_fullname = df["user_firstname"].fillna("_").str.strip() + " " + df["user_lastname"].fillna("_").str.strip())
df = df[["video_id", "user_fullname"]].drop_duplicates().sort_values(by=["video_id", "user_fullname"])

df_gr = df.groupby(by="video_id").size().to_frame("num_unique_users").reset_index()


# MySQL
# *****
WITH cte_fullnames AS
(
SELECT
    video_id,
    CONCAT( TRIM(COALESCE(user_firstname, '_')), ' ', TRIM(COALESCE(user_lastname, '_')) ) AS user_fullname
FROM user_flags
WHERE
    TRIM(flag_id) != '' AND
    flag_id IS NOT NULL
)
SELECT
    video_id,
    COUNT(DISTINCT user_fullname) AS num_unique_users
FROM cte_fullnames
GROUP BY video_id
ORDER BY video_id;



# 2.5 User with Most Approved Flags
# https://platform.stratascratch.com/coding/2104-user-with-most-approved-flags?code_type=2

# Which user flagged the most distinct videos that ended up approved by YouTube?
# Output, in one column, their full name or names in case of a tie.
# In the user's full name, include a space between the first and the last name.


# Python
# ******
import pandas as pd

df = user_flags.merge(flag_review, on="flag_id")
df = df[df["reviewed_outcome"].str.lower().eq("approved")]
df = df.assign(user_fullname = df["user_firstname"].fillna("_").str.strip() + " " + df["user_lastname"].fillna("_").str.strip())
df = df[["user_fullname", "video_id"]].drop_duplicates().sort_values(by=["user_fullname", "video_id"])

df_gr = df.groupby(by="user_fullname").size().to_frame("videos_count").reset_index()
df_gr[df_gr["videos_count"].eq(df_gr["videos_count"].max())]["user_fullname"]


# MySQL
# *****
WITH cte_appr_vid_count AS
(
SELECT
    CONCAT( TRIM(COALESCE(user_firstname, '_')), ' ', TRIM(COALESCE(user_lastname, '_')) ) AS user_fullname,
    COUNT(DISTINCT video_id) AS videos_count
FROM user_flags AS uf
JOIN flag_review AS fr
    ON uf.flag_id = fr.flag_id
WHERE LOWER(reviewed_outcome) = 'approved'
GROUP BY user_fullname
)
SELECT user_fullname
FROM cte_appr_vid_count
WHERE videos_count = (SELECT MAX(videos_count) FROM cte_appr_vid_count);



# 2.6 Find Students At Median Writing
# https://platform.stratascratch.com/coding/9610-find-students-with-a-median-writing-score?code_type=2

# Identify the IDs of students who scored exactly at the median for the SAT writing section.


# Python
# ******
import pandas as pd

sat_scores[sat_scores["sat_writing"].eq(sat_scores["sat_writing"].median())]["student_id"]


# MySQL
# *****
# Unlike Python Pandas, MySQL has no built-in MEDIAN() function.
WITH cte_row_nr AS
(
SELECT
    ROW_NUMBER() OVER(ORDER BY sat_writing) AS row_nr,
    student_id,
    sat_writing,
    COUNT(*) OVER() AS rows_count
FROM sat_scores
),
cte_median AS
(
SELECT
    AVG(sat_writing) AS s_w_median
FROM cte_row_nr
WHERE row_nr IN( FLOOR((rows_count + 1) / 2), CEIL((rows_count + 1) / 2) )
)
SELECT student_id
FROM cte_row_nr, cte_median
WHERE sat_writing = s_w_median;



# 2.7 Top 10 Songs 2010
# https://platform.stratascratch.com/coding/9650-find-the-top-10-ranked-songs-in-2010?code_type=2

# Find the top 10 ranked songs in 2010. Output the rank, group name, and song name, but do not show the same song twice.
# Sort the result based on the rank in ascending order.


# Python
# ******
import pandas as pd

df = billboard_top_100_year_end[
    billboard_top_100_year_end["year"].eq(2010) & 
    billboard_top_100_year_end["year_rank"].between(1, 10, inclusive="both")
    ][["year_rank", "group_name", "song_name"]].drop_duplicates().sort_values(by="year_rank")


# MySQL
# *****
SELECT DISTINCT
    year_rank,
    group_name,
    song_name
FROM billboard_top_100_year_end
WHERE
    year = 2010 AND
    year_rank BETWEEN 1 AND 10
ORDER BY year_rank;



# 2.8 Classify Business Type
# https://platform.stratascratch.com/coding/9726-classify-business-type?code_type=2

# Classify each business as either a restaurant, cafe, school, or other.
# - A restaurant should have the word 'restaurant' in the business name.
# - A cafe should have either 'cafe', 'café', or 'coffee' in the business name.
# - A school should have the word 'school' in the business name.
# - All other businesses should be classified as 'other'.
# Ensure each business name appears only once in the final output.
# If multiple records exist for the same business, retain only one unique instance.
# The final output should include only the distinct business names and their corresponding classifications.


# Python
# ******
import pandas as pd

sf_restaurant_health_violations = sf_restaurant_health_violations.assign(business_type = "other")

df = sf_restaurant_health_violations[["business_name", "business_type"]].drop_duplicates()

# Due to an outdated Pandas version on the server, it was not possible to apply the .case_when() method
df["business_type"] = df["business_type"].mask(df["business_name"].str.contains("restaurant", case=False, regex=False), "restaurant")
df["business_type"] = df["business_type"].mask(df["business_name"].str.contains("cafe",       case=False, regex=False), "cafe")
df["business_type"] = df["business_type"].mask(df["business_name"].str.contains("café",       case=False, regex=False), "cafe")
df["business_type"] = df["business_type"].mask(df["business_name"].str.contains("coffee",     case=False, regex=False), "cafe")
df["business_type"] = df["business_type"].mask(df["business_name"].str.contains("school",     case=False, regex=False), "school")
df


# MySQL
# *****
SELECT DISTINCT
    business_name,
    CASE
        WHEN business_name LIKE '%restaurant%' THEN 'restaurant'
        WHEN business_name LIKE '%cafe%'       THEN 'cafe'
        WHEN business_name LIKE '%café%'       THEN 'cafe'
        WHEN business_name LIKE '%coffee%'     THEN 'cafe'
        WHEN business_name LIKE '%school%'     THEN 'school'
        ELSE 'other'
    END AS business_type
FROM sf_restaurant_health_violations;
