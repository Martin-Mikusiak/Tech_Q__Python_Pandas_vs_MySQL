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
#    1.13 ***** In progress *****
#    1.14 
#    1.15 

# 2. Difficulty: Medium  (41 Questions)
#    2.1 ***** In progress *****
#    2.2 
#    2.3 
#    2.4 
#    2.5 
#    2.6 
#    2.7 
#    2.8 
#    2.9 
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

online_orders = online_orders.assign(revenue    = online_orders["cost_in_dollars"] * online_orders["units_sold"])
online_orders = online_orders.assign(date_month = online_orders["date_sold"].dt.month)

online_orders_fltr = online_orders[
    (online_orders["date_month"].ge(1)) &
    (online_orders["date_month"].le(6))
]

df_o_o_gr = online_orders_fltr.groupby(by="product_id", as_index=False)["revenue"].sum()
df_o_o_gr.nlargest(5, "revenue")


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

df_p_e_gr = playbook_events[playbook_events["device"].eq("macbook pro")].groupby(by="event_name", as_index=False)["user_id"].count()
df_p_e_gr = df_p_e_gr.rename(columns={"user_id": "event_count"})
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

worker = worker.assign(date_month = worker["joining_date"].dt.month)

worker[
    worker["department"].eq("Admin") &
    worker["date_month"].ge(4)
]["worker_id"].count()


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
SELECT
    DISTINCT hotel_name,
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



