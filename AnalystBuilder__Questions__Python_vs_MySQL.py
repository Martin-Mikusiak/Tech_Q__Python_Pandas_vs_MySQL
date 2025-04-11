# Analyst Builder - Technical Questions (only the "Free" ones)
# ************************************************************
# https://www.analystbuilder.com/questions

# My own solutions in Python Pandas and MySQL


# Contents

# 1. Difficulty: Easy  (11 Questions)
#    1.1 Tesla Models
#    1.2 Heart Attack Risk
#    1.3 Apply Discount
#    1.4 Million Dollar Store
#    1.5 Chocolate
#    1.6 On The Way Out
#    1.7 Sandwich Generation
#    1.8 Electric Bike Replacement
#    1.9 Car Failure
#    1.10 Perfect Data Analyst
#    1.11 Costco Rotisserie Loss

# 2. Difficulty: Moderate  (12 Questions)
#    2.1 Senior Citizen Discount
#    2.2 LinkedIn Famous
#    2.3 Company Wide Increase
#    2.4 Media Addicts
#    2.5 Bike Price
#    2.6 Direct Reports
#    2.7 Food Divides Us
#    2.8 Kroger's Members
#    2.9 Tech Layoffs
#    2.10 Separation
#    2.11 TMI (Too Much Information)
#    2.12 Shrink-flation

# 3. Difficulty: Hard  (4 Questions)
#    3.1 Temperature Fluctuations
#    3.2 Cake vs Pie
#    3.3 Kelly's 3rd Purchase
#    3.4 Marketing Spend



# 1. Difficulty: Easy  (11 Questions)
# ***********************************

# 1.1 Tesla Models
# https://www.analystbuilder.com/questions/tesla-models-soJdJ

# Determine which Tesla Model has made the most profit.
# Include all columns with the "profit" column at the end.


# Python
# ******
import pandas as pd

tesla_models = tesla_models.assign(profit = (tesla_models["car_price"] - tesla_models["production_cost"]) * tesla_models["cars_sold"])
tesla_models.nlargest(1, "profit")


# MySQL
# *****
SELECT
	*,
	(car_price - production_cost) * cars_sold AS profit
FROM tesla_models
ORDER BY profit DESC
LIMIT 1;



# 1.2 Heart Attack Risk
# https://www.analystbuilder.com/questions/heart-attack-risk-FKfdn

# If a patient is over the age of 50, cholesterol level of 240 or over, and weight 200 or greater, then they are at high risk of having a heart attack.
# Write a query to retrieve these patients. Include all columns in your output.
# As Cholesterol level is the largest indicator, order the output by Cholesterol from Highest to Lowest so he can reach out to them first.


# Python
# ******
import pandas as pd

patients[ 
    (patients["age"]         >   50) & 
    (patients["cholesterol"] >= 240) & 
    (patients["weight"]      >= 200) 
].sort_values(by="cholesterol", ascending=False)


# MySQL
# *****
SELECT *
FROM patients
WHERE
    age         >   50 AND
    cholesterol >= 240 AND
    weight      >= 200
ORDER BY cholesterol DESC;



# 1.3 Apply Discount
# https://www.analystbuilder.com/questions/apply-discount-RdWhb

# A Computer store is offering a 25% discount for all new customers over the age of 65 or customers that spend more than $200 on their first purchase.
# The owner wants to know how many customers received that discount since they started the promotion.
# Write a query to see how many customers received that discount.


# Python
# ******
import pandas as pd

customers[ 
    (customers["age"]            >  65) | 
    (customers["total_purchase"] > 200) 
]["customer_id"].count()


# MySQL
# *****
SELECT COUNT(customer_id)		# or:  SELECT COUNT(*)
FROM customers
WHERE
    age            >  65 OR
    total_purchase > 200;



# 1.4 Million Dollar Store
# https://www.analystbuilder.com/questions/million-dollar-store-ARdQa

# Write a query that returns all of the stores whose average yearly revenue is greater than one million dollars.
# Output the store ID and average revenue. Round the average to 2 decimal places.
# Order by store ID.


# Python
# ******
import pandas as pd

stores_gr = stores.groupby(by="store_id", as_index=False)["revenue"].mean().round(2)
stores_gr = stores_gr.rename(columns={"revenue": "avg_yearly_revenue"})
stores_gr[ stores_gr["avg_yearly_revenue"] > 1000000 ].sort_values(by="store_id")


# MySQL
# *****
SELECT
	store_id,
	ROUND(AVG(revenue), 2) AS avg_revenue
FROM stores
GROUP BY store_id
HAVING AVG(revenue) > 1000000
ORDER BY store_id;



# 1.5 Chocolate
# https://www.analystbuilder.com/questions/chocolate-vPiUY

# Write a Query to return bakery items that contain the word "Chocolate".


# Python
# ******
import pandas as pd

bakery_items[ bakery_items["product_name"].str.contains("Chocolate", case=False, regex=False) ]



# MySQL
# *****
SELECT * 
FROM bakery_items
WHERE product_name LIKE '%chocolate%';



# 1.6 On The Way Out
# https://www.analystbuilder.com/questions/on-the-way-out-LGNoQ

# Write a query to identify the IDs of the three oldest employees.
# Order output from oldest to youngest.


# Python
# ******
import pandas as pd

employees["birth_date"] = pd.to_datetime(employees["birth_date"])
employees = employees.sort_values(by="birth_date")
employees[["employee_id"]].head(3)


# MySQL
# *****
SELECT employee_id
FROM employees
ORDER BY birth_date
LIMIT 3;



# 1.7 Sandwich Generation
# https://www.analystbuilder.com/questions/sandwich-generation-excIi

# Below we have 2 tables, bread and meats.
# Output every possible combination of bread and meats.
# Order by the bread and then meat alphabetically.


# Python
# ******
import pandas as pd

bread_table.merge(meat_table, how="cross")[["bread_name", "meat_name"]].sort_values(["bread_name", "meat_name"])



# MySQL
# *****
SELECT
	bread_name,
	meat_name
FROM bread_table
CROSS JOIN meat_table
ORDER BY bread_name, meat_name;



# 1.8 Electric Bike Replacement
# https://www.analystbuilder.com/questions/electric-bike-replacement-ZaFie

# After about 10,000 miles, Electric bike batteries begin to degrade and need to be replaced.
# Write a query to determine the amount of bikes that currently need to be replaced.


# Python
# ******
import pandas as pd

bikes[ bikes["miles"] > 10000 ]["bike_id"].count()


# MySQL
# *****
SELECT COUNT(bike_id)		# or: SELECT COUNT(*)
FROM bikes
WHERE miles > 10000;



# 1.9 Car Failure
# https://www.analystbuilder.com/questions/car-failure-TUsTW

# Cars need to be inspected every year in order to pass inspection and be street legal.
# If a car has any critical issues it will fail inspection or if it has more than 3 minor issues it will also fail.
# Write a query to identify all of the cars that passed inspection.
# Output should include the owner name and vehicle name. Order by the owner name alphabetically.


# Python
# ******
import pandas as pd

insp_passed = inspections[
    (inspections["critical_issues"] == 0) & 
    (inspections["minor_issues"]    <= 3)
]

insp_passed[["owner_name", "vehicle"]].sort_values(by="owner_name")


# MySQL
# *****
SELECT
	owner_name,
	vehicle
FROM inspections
WHERE
    critical_issues  = 0 AND
    minor_issues    <= 3
ORDER BY owner_name;



# 1.10 Perfect Data Analyst
# https://www.analystbuilder.com/questions/perfect-data-analyst-GMFmx

# Return all the candidate IDs that have problem solving skills, SQL experience, knows Python or R, and has domain knowledge.
# Order output on IDs from smallest to largest.


# Python
# ******
import pandas as pd

candidates[
    (candidates["problem_solving"]  == "X") &
    (candidates["sql_experience"]   == "X") &
    ( (candidates["python"] == "X") | (candidates["r_programming"] == "X") ) &
    (candidates["domain_knowledge"] == "X")
][["candidate_id"]].sort_values(by="candidate_id")


# MySQL
# *****
SELECT candidate_id
FROM candidates
WHERE
	problem_solving  = 'X' AND
	sql_experience   = 'X' AND
	(python = 'X' OR r_programming = 'X') AND
	domain_knowledge = 'X'
ORDER BY candidate_id;



# 1.11 Costco Rotisserie Loss
# https://www.analystbuilder.com/questions/costco-rotisserie-loss-kkCDh

# Using the sales table, calculate how much money they have lost on their rotisserie chickens this year. Round to the nearest whole number.


# Python
# ******
import pandas as pd

total_loss = sales["lost_revenue_millions"].sum().round().astype(int)


# MySQL
# *****
SELECT ROUND(SUM(lost_revenue_millions)) AS total_loss
FROM sales;





# 2. Difficulty: Moderate  (12 Questions)
# ***************************************

# 2.1 Senior Citizen Discount
# https://www.analystbuilder.com/questions/senior-citizen-discount-fRxVJ

# If a customer is 55 or above they qualify for the senior citizen discount. Check which customers qualify.
# Assume the current date 1/1/2023.
# Return all of the Customer IDs who qualify for the senior citizen discount in ascending order.


# Python
# ******
import pandas as pd

date_compare = pd.to_datetime("2023-01-01")
customers["birth_date"] = pd.to_datetime(customers["birth_date"])
customers = customers.assign(age = (date_compare - customers["birth_date"]).dt.days // 365.2425)
customers[ customers["age"] >= 55 ][["customer_id"]].sort_values(by="customer_id")


# MySQL
# *****
SELECT customer_id
FROM customers
WHERE TIMESTAMPDIFF(YEAR, birth_date, '2023-01-01') >= 55
ORDER BY customer_id;



# 2.2 LinkedIn Famous
# https://www.analystbuilder.com/questions/linkedin-famous-oQMdb

# Write a query to determine the popularity of a post on LinkedIn.
# Popularity is defined by number of actions (likes, comments, shares, etc.) divided by the number impressions the post received * 100.
# If the post receives a score higher than 1 it was very popular.
# Return all the post IDs and their popularity where the score is 1 or greater.
# Order popularity from highest to lowest.


# Python
# ******
import pandas as pd

linkedin_posts = linkedin_posts.assign(popularity = (linkedin_posts["actions"] / linkedin_posts["impressions"] * 100).round(4))
linkedin_posts[ linkedin_posts["popularity"] >= 1 ][["post_id", "popularity"]].sort_values(by="popularity", ascending=False)


# MySQL
# *****
SELECT
    post_id,
    ROUND((actions / impressions * 100), 4) AS popularity
FROM linkedin_posts
WHERE (actions / impressions * 100) >= 1
ORDER BY popularity DESC;



# 2.3 Company Wide Increase
# https://www.analystbuilder.com/questions/company-wide-increase-TytwW

# If our company hits its yearly targets, every employee receives a salary increase depending on what Level you are in the company.
# Give each employee who is a Level 1 a 10% increase, Lvel 2 a 15% increase, and Level 3 a 200% increase.
# Include this new column in your output as "new_salary" along with your other columns.


# Python
# ******
import pandas as pd

case_list_incr = [
    (employees["pay_level"].eq(1), employees["salary"] * 1.1 ),
    (employees["pay_level"].eq(2), employees["salary"] * 1.15),
    (employees["pay_level"].eq(3), employees["salary"] * 3   )
]

employees = employees.assign(new_salary = employees["salary"])
employees["new_salary"] = employees["new_salary"].case_when(case_list_incr)
employees


# MySQL
# *****
SELECT
	*,
	CASE pay_level
		WHEN 1 THEN salary * 1.1
		WHEN 2 THEN salary * 1.15
		WHEN 3 THEN salary * 3
		ELSE salary
	END AS new_salary
FROM employees;



# 2.4 Media Addicts
# https://www.analystbuilder.com/questions/media-addicts-deISZ

# Write a query to find the people who spent a higher than average amount of time on social media.
# Provide just their first names alphabetically so we can reach out to them individually.


# Python
# ******
import pandas as pd

avg_mtm = user_time["media_time_minutes"].mean()
df_uid_flt = user_time[ user_time["media_time_minutes"] > avg_mtm ][["user_id"]]
df_uid_flt.merge(users, on="user_id")[["first_name"]].sort_values(by="first_name")


# MySQL
# *****
WITH cte_uid AS
(
SELECT user_id
FROM user_time
WHERE media_time_minutes > (SELECT AVG(media_time_minutes) FROM user_time)
)
SELECT first_name
FROM users
JOIN cte_uid
	ON users.user_id = cte_uid.user_id
ORDER BY first_name;

# Alternative MySQL code:  ... WHERE user_id IN (SELECT ...)



# 2.5 Bike Price
# https://www.analystbuilder.com/questions/bike-price-zKcOR

# Sarah's Bike Shop sells a lot of bikes and wants to know what the average sale price is of her bikes.
# She sometimes gives away a bike for free for a charity event and if she does she leaves the price of the bike as blank, but marks it sold.
# Write a query to show her the average sale price of bikes for only bikes that were sold, and not donated.
# Round answer to 2 decimal places.


# Python
# ******
import pandas as pd

avg_bike_price = inventory[ 
    (inventory["bike_price"].notnull()) & 
    (inventory["bike_sold" ] == "Y") 
]["bike_price"].mean().round(2)


# MySQL
# *****
SELECT ROUND(AVG(bike_price), 2) AS avg_bike_price
FROM inventory
WHERE
    bike_price IS NOT NULL AND
    bike_sold = 'Y';



# 2.6 Direct Reports
# https://www.analystbuilder.com/questions/direct-reports-qQoVA

# Write a query to determine how many direct reports each Manager has.
# Note: Managers will have "Manager" in their title.
# Report the Manager ID, Manager Title, and the number of direct reports in your output.


# Python
# ******
import pandas as pd

dr_count = direct_reports.groupby(by="managers_id", as_index=False)["employee_id"].count()
dr_count = dr_count.rename(columns={"managers_id": "manager_id", "employee_id": "direct_reports_count"})

mng = direct_reports[ direct_reports["position"].str.contains("Manager", case=False, regex=False) ][["employee_id", "position"]]
mng = mng.rename(columns={"employee_id": "manager_id", "position": "manager_title"})

mng.merge(dr_count, on="manager_id", how="left")


# MySQL
# *****
# Solution #1 - using a CTE and then JOIN
WITH cte_dr_count AS
(
SELECT
    managers_id,
    COUNT(*) AS direct_reports_count
FROM direct_reports
WHERE managers_id IS NOT NULL
GROUP BY managers_id
)
SELECT
    employee_id AS manager_id,
    position    AS manager_title,
    direct_reports_count
FROM direct_reports
JOIN cte_dr_count
    ON employee_id = cte_dr_count.managers_id
WHERE position LIKE '%Manager%';


# Solution #2 - using a Self-JOIN
SELECT
    dr_m.employee_id AS manager_id,
    dr_m.position    AS manager_title,
    COUNT(*)         AS direct_reports_count
FROM direct_reports AS dr_e
JOIN direct_reports AS dr_m
    ON dr_e.managers_id = dr_m.employee_id
WHERE dr_m.position LIKE '%Manager%'
GROUP BY manager_id;



# 2.7 Food Divides Us
# https://www.analystbuilder.com/questions/food-divides-us-GvhLL

# Write a query to determine which region spends the most amount of money on fast food.


# Python
# ******
import pandas as pd

regions_gr = food_regions.groupby(by="region", as_index=False, sort=False)["fast_food_millions"].sum()
regions_gr = regions_gr.rename(columns={"fast_food_millions": "sum_millions"})
regions_gr.nlargest(1, "sum_millions")[["region"]]


# MySQL
# *****
SELECT region
FROM food_regions
GROUP BY region
ORDER BY SUM(fast_food_millions) DESC
LIMIT 1;

# Alternative code:  Using a CTE ...



# 2.8 Kroger's Members
# https://www.analystbuilder.com/questions/krogers-members-FjyKN

# Write a query to find the percentage of customers who shop at Kroger's who also have a Kroger's membership card. Round to 2 decimal places.


# Python
# ******
import pandas as pd

percentage = ( customers[ customers["has_member_card"] == "Y" ]["has_member_card"].count() / customers["kroger_id"].count() * 100 ).round(2)


# MySQL
# *****
SELECT
	ROUND(COUNT(has_member_card) / (SELECT COUNT(*) FROM customers) * 100, 2) AS percentage
FROM customers
WHERE has_member_card = 'Y';



# 2.9 Tech Layoffs
# https://www.analystbuilder.com/questions/tech-layoffs-CpLXE

# Write a query to determine the percentage of employees that were laid off from each company.
# Output should include the company and the percentage (to 2 decimal places) of laid off employees.
# Order by company name alphabetically.


# Python
# ******
import pandas as pd

tech_layoffs = tech_layoffs.assign(pct_laid_off = ( tech_layoffs["employees_fired"] / tech_layoffs["company_size"] * 100 ).round(2))
tech_layoffs[["company", "pct_laid_off"]].sort_values(by="company")


# MySQL
# *****
SELECT
	company,
	ROUND(employees_fired / company_size * 100, 2) AS pct_laid_off
FROM tech_layoffs
ORDER BY company;



# 2.10 Separation
# https://www.analystbuilder.com/questions/separation-DbHMu

# Data was input incorrectly into the database. The ID was combined with the First Name.
# Write a query to separate the ID and First Name into two separate columns.
# Each ID is 5 characters long.


# Python
# ******
import pandas as pd

bad_data["first_name"] = bad_data["id"].str[5:]
bad_data["id"] = bad_data["id"].str[:5]
df_cleaned = bad_data


# MySQL
# *****
SELECT
	LEFT(id, 5)   AS id,
	SUBSTR(id, 6) AS first_name
FROM bad_data;



# 2.11 TMI (Too Much Information)
# https://www.analystbuilder.com/questions/tmi-too-much-information-VyNhZ

# Here you are given a table that contains a customer ID and their full name.
# Return the customer ID with only the first name of each customer.


# Python
# ******
import pandas as pd

customers[["first_name", "last_name"]] = customers["full_name"].str.split(n=1, expand=True)
customers[["customer_id", "first_name"]]


# MySQL
# *****
SELECT
	customer_id,
	SUBSTRING_INDEX(full_name, ' ', 1) AS first_name
FROM customers;



# 2.12 Shrink-flation
# https://www.analystbuilder.com/questions/shrink-flation-ohNJw

# Write a query to identify products that have undergone shrink-flation over the last year.
# Shrink-flation is defined as a reduction in product size while maintaining or increasing the price.
# Include a flag for Shrinkflation. This should be a boolean value (True or False) indicating whether the product has undergone shrink-flation.
# The output should have the columns Product_Name, Size_Change_Percentage, Price_Change_Percentage, and Shrinkflation_Flag.
# Round percentages to the nearest whole number and order the output on the product names alphabetically.


# Python
# ******
import pandas as pd

products = products.assign( size_change_percentage = ( (products["new_size" ] - products["original_size" ]) / products["original_size" ] * 100 ).round() )
products = products.assign(price_change_percentage = ( (products["new_price"] - products["original_price"]) / products["original_price"] * 100 ).round() )

products["shrinkflation_flag"] = (
    products["new_size" ].lt(products["original_size" ]) &
    products["new_price"].ge(products["original_price"])
).astype(str)

products[["product_name", "size_change_percentage", "price_change_percentage", "shrinkflation_flag"]].sort_values(by="product_name")


# MySQL
# *****
SELECT
	product_name,
	ROUND((new_size  - original_size ) / original_size  * 100) AS  size_change_percentage,
	ROUND((new_price - original_price) / original_price * 100) AS price_change_percentage,
	CASE
		WHEN new_size < original_size AND new_price >= original_price THEN 'True'
		ELSE 'False'
	END AS shrinkflation_flag
FROM products
ORDER BY product_name;





# 3. Difficulty: Hard  (4 Questions)
# **********************************

# 3.1 Temperature Fluctuations
# https://www.analystbuilder.com/questions/temperature-fluctuations-ftFQu

# Write a query to find all dates with higher temperatures compared to the previous dates (yesterday).
# Order dates in ascending order.


# Python
# ******
import pandas as pd

temperatures[ temperatures["temperature"].diff().gt(0) ][["date"]].sort_values(by="date")


# MySQL
# *****
WITH cte_t_diff AS
(
SELECT
    *,
    temperature - LAG(temperature) OVER(ORDER BY date) AS t_diff
FROM temperatures
)
SELECT date
FROM cte_t_diff
WHERE t_diff > 0;



# 3.2 Cake vs Pie
# https://www.analystbuilder.com/questions/cake-vs-pie-rSDbF

# Marcie's Bakery is having a contest at her store. Whichever dessert sells more each day will be on discount tomorrow.
# She needs to identify which dessert is selling more.
# Write a query to report the difference between the number of Cakes and Pies sold each day.
# Output should include the date sold, the difference between cakes and pies, and which one sold more (cake or pie).
# The difference should be a positive number.
# Return the result table ordered by Date_Sold.
# Columns in output should be date_sold, difference, and sold_more.


# Python
# ******
import pandas as pd

desserts["amount_sold"] = desserts["amount_sold"].fillna(0).astype(int)
df = desserts.pivot(index="date_sold", columns="product", values="amount_sold")
df = df.reset_index().rename_axis(None, axis=1)
df = df.assign(difference = df["Cake"] - df["Pie"])

case_list_diff = [
    (df["difference"].lt(0), "Pie" ),
    (df["difference"].gt(0), "Cake")
]

df = df.assign(sold_more = "Equal")
df["sold_more"] = df["sold_more"].case_when(case_list_diff)

df["difference"] = df["difference"].abs()
df[["date_sold", "difference", "sold_more"]].sort_values(by="date_sold")


# MySQL
# *****
# Solution #1 - using a CTE / pivot
WITH cte_pivot AS
(
SELECT
    date_sold,
    SUM(CASE WHEN product = 'Cake' THEN IFNULL(amount_sold, 0) END) AS c_sold,
    SUM(CASE WHEN product = 'Pie'  THEN IFNULL(amount_sold, 0) END) AS p_sold
FROM desserts
GROUP BY date_sold
ORDER BY date_sold
)
SELECT
    date_sold,
    ABS(c_sold - p_sold) AS difference,
    CASE
        WHEN c_sold > p_sold THEN 'Cake'
        WHEN c_sold < p_sold THEN 'Pie'
        ELSE 'Equal'
    END AS sold_more
FROM cte_pivot;


# Solution #2 - using a Self-JOIN
SELECT
	d1.date_sold,
	ABS(IFNULL(d1.amount_sold, 0) - IFNULL(d2.amount_sold, 0)) AS difference,
	CASE
		WHEN IFNULL(d1.amount_sold, 0) > IFNULL(d2.amount_sold, 0) THEN d1.product
		WHEN IFNULL(d1.amount_sold, 0) < IFNULL(d2.amount_sold, 0) THEN d2.product
		ELSE 'Equal'
	END AS sold_more
FROM desserts AS d1
JOIN desserts AS d2
	ON d1.date_sold = d2.date_sold
	AND d1.product = 'Cake'
	AND d2.product = 'Pie'
ORDER BY d1.date_sold;



# 3.3 Kelly's 3rd Purchase
# https://www.analystbuilder.com/questions/kellys-3rd-purchase-kFaIE

# At Kelly's Ice Cream Shop, Kelly gives a 33% discount on each customer's 3rd purchase.
# Write a query to select the 3rd transaction for each customer that received that discount.
# Output the customer id, transaction id, amount, and the amount after the discount as "discounted_amount".
# Order output on customer ID in ascending order.
# Note: Transaction IDs occur sequentially. The lowest transaction ID is the earliest ID.


# Python
# ******
import pandas as pd

purchases["rank"] = purchases.groupby("customer_id")["transaction_id"].rank(method="first", ascending=True)

purch_3rd = purchases[purchases["rank"] == 3].sort_values("customer_id")
purch_3rd["discounted_amount"] = (purch_3rd["amount"] * 0.67).round(2)
purch_3rd = purch_3rd.drop(columns="rank")
purch_3rd


# MySQL
# *****
WITH cte_row_n AS
(
SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY transaction_id) AS row_n
FROM purchases
)
SELECT
	customer_id,
	transaction_id,
	amount,
	amount * 0.67 AS discounted_amount
FROM cte_row_n
WHERE row_n = 3
ORDER BY customer_id;



# 3.4 Marketing Spend
# https://www.analystbuilder.com/questions/marketing-spend-mrTJL

# Calculate the Return on Investment (ROI) for each campaign and identify the top 25% of campaigns
# that have the highest ROI. Round ROI to the nearest whole number.
# The output should have the columns Campaign_ID, Campaign_Name, ROI
# and should be ordered by ROI in descending order and Campaign_ID in descending order.
# Only include the top 25% of campaigns with the highest ROI.


# Python
# ******
# Solution #1 - using the nlargest() method - faster, but outputs only the first quantile
import pandas as pd

marketing_spend["roi_pct"] = ( ( marketing_spend["revenue_generated"] - marketing_spend["investment"] ) / marketing_spend["investment"] * 100 ).round().astype(int)
lmt_rows = round(marketing_spend.shape[0] / 4)    # number of rows in the output
marketing_spend[["campaign_id", "campaign_name", "roi_pct"]].nlargest(lmt_rows, ["roi_pct", "campaign_id"])


# Solution #2 - using the pandas.qcut() quantile-based function - slower, but then can output any of the selected quantile(s)
import pandas as pd

marketing_spend["roi_pct"] = ( ( marketing_spend["revenue_generated"] - marketing_spend["investment"] ) / marketing_spend["investment"] * 100 ).round().astype(int)

# pandas.qcut() function: adding a new column "ntile_4", while the values of "roi_pct" column are automatically sorted in ascending order
marketing_spend["ntile_4"] = pd.qcut(marketing_spend["roi_pct"], q=4, labels=False) + 1    # adding +1 for 1..n based numbering of the quantiles
marketing_spend[ marketing_spend["ntile_4"].eq(4) ].sort_values(by=["roi_pct", "campaign_id"], ascending=[False, False])[["campaign_id", "campaign_name", "roi_pct"]]


# MySQL
# *****
WITH cte_roi AS
(
SELECT
    campaign_id,
    campaign_name,
    ROUND((revenue_generated - investment) / investment * 100) AS roi_pct
FROM marketing_spend
),
cte_ntile_4 AS
(
SELECT
    *,
    NTILE(4) OVER(ORDER BY roi_pct DESC, campaign_id DESC) AS ntile_4_num
FROM cte_roi
)
SELECT
    campaign_id,
    campaign_name,
    roi_pct
FROM cte_ntile_4
WHERE ntile_4_num = 1;
