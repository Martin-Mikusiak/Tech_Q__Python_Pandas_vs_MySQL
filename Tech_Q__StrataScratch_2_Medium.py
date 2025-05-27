# StrataScratch - Technical Questions (only the "Free" ones)
# **********************************************************
# https://platform.stratascratch.com/coding?code_type=2&is_freemium=1

# My own solutions - both in Python Pandas and in MySQL

# ***** In progress *****


# Contents

# 1. Difficulty: Easy  (27 Questions)
# See the previous file "Tech_Q__StrataScratch_1_Easy.py"

# 2. Difficulty: Medium  (41 Questions)
#    2.1 Share of Active Users
#    2.2 Premium Accounts
#    2.3 Election Results
#    2.4 Flags per Video
#    2.5 User with Most Approved Flags
#    2.6 Find Students At Median Writing
#    2.7 Top 10 Songs 2010
#    2.8 Classify Business Type
#    2.9 Processed Ticket Rate By Type
#    2.10 Customer Revenue In March
#    2.11 Count Occurrences Of Words In Drafts
#    2.12 Titanic Survivors and Non-Survivors
#    2.13 Second Highest Salary
#    2.14 Employee and Manager Salaries
#    2.15 Highest Salary In Department
#    2.16 Highest Target Under Manager
#    2.17 Highest Number Of Orders
#    2.18 Highest Cost Orders
#    2.19 Largest Olympics
#    2.20 Aroma-based Winery Search
#    2.21 Top Businesses With Most Reviews
#    2.22 Reviews of Categories
#    2.23 Top Cool Votes
#    2.24 Income By Title and Gender
#    2.25 Matching Similar Hosts and Guests
#    2.26 Find the Percentage of Shipable Orders
#    2.27 Spam Posts
#    2.28 Apple Product Counts
#    2.29 No Order Customers
#    2.30 Number Of Units Per Nationality
#    2.31 Ranking Most Active Guests
#    2.32 Number of Streets Per Zip Code
#    2.33 ***** In progress *****
#    2.34 
#    2.35 



# 3. Difficulty: Hard  (12 Questions)
#    3.1 ***** In progress *****
#    3.2 
#    3.3 



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
    USING (flag_id)
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
# Unlike Python Pandas, MySQL has no built-in MEDIAN() function - it has to be calculated the "Hard way" using 2 CTEs.
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

# Due to an outdated Pandas version on the StrataScratch server, it was not possible to apply the .case_when() method
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



# 2.9 Processed Ticket Rate By Type
# https://platform.stratascratch.com/coding/9781-find-the-rate-of-processed-tickets-for-each-type?code_type=2

# Find the processed rate of tickets for each type.
# The processed rate is defined as the number of processed tickets divided by the total number of tickets for that type.
# Round this result to two decimal places.


# Python
# ******
# Solution #1 - Simple
import pandas as pd

df = facebook_complaints.groupby(by="type", as_index=False).agg(processed_rate = ("processed", "mean")).round(2)


# Solution #2 - The "Hard way"
import pandas as pd

df = facebook_complaints.groupby(by="type", as_index=False).agg(
    pr_true  = ("processed"   , "sum"  ),
    pr_count = ("complaint_id", "count"))

df = df.assign(processed_rate = (df["pr_true"] / df["pr_count"]).round(2))
df[["type", "processed_rate"]]


# MySQL
# *****
SELECT
    type,
    ROUND(AVG(processed), 2) AS processed_rate
FROM facebook_complaints
GROUP BY type;



# 2.10 Customer Revenue In March
# https://platform.stratascratch.com/coding/9782-customer-revenue-in-march?code_type=2

# Calculate the total revenue from each customer in March 2019. Include only customers who were active in March 2019.
# Output the revenue along with the customer id and sort the results based on the revenue in descending order.


# Python
# ******
import pandas as pd

df = orders[
    orders["order_date"].dt.strftime("%Y%m").eq("201903")
    ].groupby(by="cust_id", as_index=False).agg(total_revenue = ("total_order_cost", "sum")).sort_values(by="total_revenue", ascending=False)


# MySQL
# *****
SELECT
    cust_id,
    SUM(total_order_cost) AS total_revenue
FROM orders
WHERE EXTRACT(YEAR_MONTH FROM order_date) = 201903
GROUP BY cust_id
ORDER BY total_revenue DESC;



# 2.11 Count Occurrences Of Words In Drafts
# https://platform.stratascratch.com/coding/9817-find-the-number-of-times-each-word-appears-in-drafts?code_type=2

# Find the number of times each word appears in the contents column across all rows in the drafts dataset.
# Output two columns: word and occurrences.


# Python
# ******
import pandas as pd

df_words = google_file_store["contents"].str.lower().str.split().explode().str.replace(r"[.,!?]", "", regex=True).to_frame("word")

df_gr = df_words.groupby(by="word").size().to_frame("occurrences").reset_index().sort_values(by=["occurrences", "word"], ascending=[False, True])


# MySQL
# *****
WITH cte_cl_words AS
(
SELECT
    LOWER( TRIM( REGEXP_REPLACE(jt.word, '[.,!?]', '') ) ) AS cl_word
FROM google_file_store,    
JSON_TABLE( CONCAT('["', REPLACE(contents, ' ', '","'), '"]'), "$[*]" COLUMNS (word VARCHAR(100) PATH "$") ) AS jt
WHERE contents IS NOT NULL
)
SELECT
    cl_word AS word,
    COUNT(*) AS occurrences
FROM cte_cl_words
GROUP BY word
ORDER BY occurrences DESC, word;



# 2.12 Titanic Survivors and Non-Survivors
# https://platform.stratascratch.com/coding/9881-make-a-report-showing-the-number-of-survivors-and-non-survivors-by-passenger-class?code_type=2

# Make a report showing the number of survivors and non-survivors by passenger class.
# Classes are categorized based on the pclass column value.
# Output the number of survivors and non-survivors by each class.


# Python
# ******
# Solution #1 - Using Pandas.crosstab() - Simple
import pandas as pd

df_pivot = pd.crosstab(index=titanic["survived"], columns=titanic["pclass"]).reset_index().rename(columns={1: "1st_class", 2: "2nd_class", 3: "3rd_class"})


# Solution #2 - Using .groupby().size() and then .pivot() - The "Hard way"
import pandas as pd

df_gr = titanic.groupby(by=["survived", "pclass"]).size().to_frame("count").reset_index()

df_pivot = df_gr.pivot(index="survived", columns="pclass", values="count")
df_pivot = df_pivot.reset_index().rename_axis(None, axis=1).rename(columns={1: "1st_class", 2: "2nd_class", 3: "3rd_class"})


# MySQL
# *****
SELECT
    survived,
    SUM( CASE WHEN pclass = 1 THEN 1 END ) AS 1st_class,
    SUM( CASE WHEN pclass = 2 THEN 1 END ) AS 2nd_class,
    SUM( CASE WHEN pclass = 3 THEN 1 END ) AS 3rd_class
FROM titanic
GROUP BY survived
ORDER BY survived;



# 2.13 Second Highest Salary
# https://platform.stratascratch.com/coding/9892-second-highest-salary?code_type=2

# Find the second highest salary of employees.


# Python
# ******
import pandas as pd

employee = employee.assign(s_rank = employee["salary"].rank(method="dense", ascending=False))
second_highest_salary = employee[employee["s_rank"].eq(2)][["salary"]].head(1)


# MySQL
# *****
WITH cte_s_rank AS
(
SELECT
    salary,
    DENSE_RANK() OVER(ORDER BY salary DESC) AS s_rank
FROM employee
)
SELECT DISTINCT salary AS second_highest_salary
FROM cte_s_rank
WHERE s_rank = 2;



# 2.14 Employee and Manager Salaries
# https://platform.stratascratch.com/coding/9894-employee-and-manager-salaries?code_type=2

# Find employees who are earning more than their managers.
# Output the employee's first name along with the corresponding salary.


# Python
# ******
import pandas as pd

df = employee.merge(employee, how="left", left_on="manager_id", right_on="id", suffixes=("_L", "_R"))
df[df["salary_L"].gt(df["salary_R"])][["first_name_L", "salary_L"]]


# MySQL
# *****
SELECT
    e1.first_name,
    e1.salary
FROM employee AS e1
LEFT JOIN employee AS e2
    ON e1.manager_id = e2.id
WHERE e1.salary > e2.salary;



# 2.15 Highest Salary In Department
# https://platform.stratascratch.com/coding/9897-highest-salary-in-department?code_type=2

# Find the employee with the highest salary per department.
# Output the department name, employee's first name along with the corresponding salary.


# Python
# ******
import pandas as pd

employee = employee.assign(d_s_rank = employee.groupby(by="department")["salary"].rank(method="dense", ascending=False))
employee[employee["d_s_rank"].eq(1)][["department", "first_name", "salary"]].sort_values(by="department")


# MySQL
# *****
WITH cte_d_s_rank AS
(
SELECT
    department,
    first_name,
    salary,
    DENSE_RANK() OVER(PARTITION BY department ORDER BY salary DESC) AS d_s_rank
FROM employee
)
SELECT
    department,
    first_name,
    salary
FROM cte_d_s_rank
WHERE d_s_rank = 1;



# 2.16 Highest Target Under Manager
# https://platform.stratascratch.com/coding/9905-highest-target-under-manager?code_type=2

# Identify the employee(s) working under manager manager_id=13 who have achieved the highest target.
# Return each such employee’s first name alongside the target value.
# The goal is to display the maximum target among all employees under manager_id=13 and show which employee(s) reached that top value.


# Python
# ******
import pandas as pd

df = salesforce_employees[salesforce_employees["manager_id"].eq(13)][["first_name", "target"]]
df[df["target"].eq(df["target"].max())]


# MySQL
# *****
SELECT
    first_name,
    target
FROM salesforce_employees
WHERE 
    manager_id = 13 AND
    target = (SELECT MAX(target) FROM salesforce_employees WHERE manager_id = 13);



# 2.17 Highest Number Of Orders
# https://platform.stratascratch.com/coding/9909-highest-number-of-orders?code_type=2

# Find the customer who has placed the highest number of orders.
# Output the id of the customer along with the corresponding number of orders.


# Python
# ******
# Solution #1
import pandas as pd

df_gr = orders.groupby(by="cust_id", as_index=False)["id"].nunique().rename(columns={"id": "orders_count"}).sort_values(by="orders_count", ascending=False)
df_gr[df_gr["orders_count"].eq(df_gr["orders_count"].max())]


# Solution #2
import pandas as pd

df_gr = orders.drop_duplicates().groupby(by="cust_id").size().to_frame("orders_count").reset_index().sort_values(by="orders_count", ascending=False)
df_gr[df_gr["orders_count"].eq(df_gr["orders_count"].max())]


# MySQL
# *****
WITH cte_orders_count AS
(
SELECT
    cust_id,
    COUNT(DISTINCT id) AS orders_count
FROM orders
GROUP BY cust_id
ORDER BY orders_count DESC
)
SELECT *
FROM cte_orders_count
WHERE orders_count = (SELECT MAX(orders_count) FROM cte_orders_count);



# 2.18 Highest Cost Orders
# https://platform.stratascratch.com/coding/9915-highest-cost-orders?code_type=2

# Find the customer with the highest daily total order cost between 2019-02-01 to 2019-05-01.
# If a customer had more than one order on a certain day, sum the order costs on daily basis.
# Output customer's first name, total cost of their items, and the date.


# Python
# ******
import pandas as pd

df_gr = orders[orders["order_date"].between("2019-02-01", "2019-05-01")].groupby(["order_date", "cust_id"], as_index=False)["total_order_cost"].sum()

df_mrg = df_gr.merge(customers, left_on="cust_id", right_on="id")[["order_date", "cust_id", "total_order_cost", "first_name"]]

df_mrg[df_mrg["total_order_cost"].eq(df_mrg["total_order_cost"].max())][["first_name", "order_date", "total_order_cost"]]


# MySQL
# *****
WITH cte_sum AS
(
SELECT
    order_date,
    cust_id,
    SUM(total_order_cost) AS sum_order_cost
FROM orders
WHERE order_date BETWEEN '2019-02-01' AND '2019-05-01'
GROUP BY order_date, cust_id
ORDER BY order_date, cust_id
)
SELECT
    first_name,
    order_date,
    sum_order_cost
FROM cte_sum AS s
JOIN customers AS c
    ON s.cust_id = c.id
WHERE sum_order_cost = (SELECT MAX(sum_order_cost) FROM cte_sum);



# 2.19 Largest Olympics
# https://platform.stratascratch.com/coding/9942-largest-olympics?code_type=2

# Find the Olympics with the highest number of unique athletes.
# The Olympics game is a combination of the year and the season, and is found in the games column.
# Output the Olympics along with the corresponding number of athletes. The id column uniquely identifies an athlete.


# Python
# ******
import pandas as pd

df = olympics_athletes_events.groupby(by="games", as_index=False)["id"].nunique().rename(columns={"id": "athletes_count"})
df.nlargest(1, "athletes_count", keep="all")


# MySQL
# *****
WITH cte_a_count AS
(
SELECT
    games,
    COUNT(DISTINCT id) AS athletes_count
FROM olympics_athletes_events
GROUP BY games
)
SELECT *
FROM cte_a_count
WHERE athletes_count = (SELECT MAX(athletes_count) FROM cte_a_count);



# 2.20 Aroma-based Winery Search
# https://platform.stratascratch.com/coding/10026-find-all-wineries-which-produce-wines-by-possessing-aromas-of-plum-cherry-rose-or-hazelnut?code_type=2

# Find wineries producing wines with aromas of plum, cherry, rose, or hazelnut (singular form only).
# Substring matches, like plums, cherries should be excluded.


# Python
# ******
import pandas as pd

winemag_p1[
    winemag_p1["description"].str.contains(r"\b(plum|cherry|rose|hazelnut)\b", case=False, regex=True)
    ][["winery"]].drop_duplicates().sort_values(by="winery")


# MySQL
# *****
SELECT DISTINCT winery
FROM winemag_p1
WHERE LOWER(description) REGEXP '\\b(plum|cherry|rose|hazelnut)\\b'
ORDER BY winery;



# 2.21 Top Businesses With Most Reviews
# https://platform.stratascratch.com/coding/10048-top-businesses-with-most-reviews?code_type=2

# Find the top 5 businesses with most reviews.
# Assume that each row has a unique business_id such that the total reviews for each business is listed on each row.
# Output the business name along with the total number of reviews and order your results by the total reviews in descending order.


# Python
# ******
import pandas as pd

yelp_business.nlargest(5, "review_count", keep="all")[["name", "review_count"]]


# MySQL
# *****
WITH cte_d_rank AS
(
SELECT
    name,
    review_count,
    DENSE_RANK() OVER(ORDER BY review_count DESC) AS d_rank
FROM yelp_business
)
SELECT
    name,
    review_count
FROM cte_d_rank
WHERE d_rank <= 5;



# 2.22 Reviews of Categories
# https://platform.stratascratch.com/coding/10049-reviews-of-categories?code_type=2

# Calculate number of reviews for every business category.
# Output the category along with the total number of reviews.
# Order by total reviews in descending order.


# Python
# ******
import pandas as pd

df = yelp_business[["review_count", "categories"]]
df = df.assign(ctgr = df["categories"].str.split(";")).explode("ctgr")

df_gr = df.groupby(by="ctgr", as_index=False)["review_count"].sum().sort_values(by="review_count", ascending=False)


# MySQL
# *****
WITH cte_ctgr AS
(
SELECT 
    ctgr,
    review_count
FROM yelp_business,
JSON_TABLE( CONCAT('["', REPLACE(categories, ';', '","'), '"]'), "$[*]" COLUMNS (ctgr VARCHAR(100) PATH "$") ) AS jt
)
SELECT
    ctgr AS category,
    SUM(review_count) AS sum_review_count
FROM cte_ctgr
GROUP BY category
ORDER BY sum_review_count DESC;



# 2.23 Top Cool Votes
# https://platform.stratascratch.com/coding/10060-top-cool-votes?code_type=2

# Find the review_text that received the highest number of  cool votes.
# Output the business name along with the review text with the highest number of cool votes.


# Python
# ******
import pandas as pd

yelp_reviews[yelp_reviews["cool"].eq(yelp_reviews["cool"].max())][["business_name", "review_text"]]


# MySQL
# *****
SELECT
    business_name,
    review_text
FROM yelp_reviews
WHERE cool = (SELECT MAX(cool) FROM yelp_reviews);



# 2.24 Income By Title and Gender
# https://platform.stratascratch.com/coding/10077-income-by-title-and-gender?code_type=2

# Find the average total compensation based on employee titles and gender.
# Total compensation is calculated by adding both the salary and bonus of each employee.
# However, not every employee receives a bonus so disregard employees without bonuses in your calculation.
# Employee can receive more than one bonus.


# Python
# ******
import pandas as pd

bonus_gr = sf_bonus.groupby(by="worker_ref_id")["bonus"].sum().to_frame("bonus_sum").reset_index()

df = sf_employee.merge(bonus_gr, left_on="id", right_on="worker_ref_id")
df = df.assign(total_comp = df["salary"] + df["bonus_sum"])

df_gr = df.groupby(by=["employee_title", "sex"])["total_comp"].mean().to_frame("avg_total_comp").reset_index()


# MySQL
# *****
WITH cte_bonus_gr AS
(
SELECT
    worker_ref_id,
    SUM(bonus) AS bonus_sum
FROM sf_bonus
GROUP BY worker_ref_id
)
SELECT
    employee_title,
    sex,
    AVG(salary + bonus_sum) AS avg_total_comp
FROM sf_employee AS e
JOIN cte_bonus_gr AS b
    ON e.id = b.worker_ref_id
GROUP BY employee_title, sex;



# 2.25 Matching Similar Hosts and Guests
# https://platform.stratascratch.com/coding/10078-find-matching-hosts-and-guests-in-a-way-that-they-are-both-of-the-same-gender-and-nationality?code_type=2

# Find matching hosts and guests pairs in a way that they are both of the same gender and nationality.
# Output the host id and the guest id of matched pair.


# Python
# ******
import pandas as pd

df_m = airbnb_hosts.drop_duplicates().merge(airbnb_guests.drop_duplicates(), on=["nationality", "gender"])[["host_id", "guest_id"]]


# MySQL
# *****
SELECT DISTINCT
    host_id,
    guest_id
FROM airbnb_hosts
JOIN airbnb_guests
    USING (nationality, gender);



# 2.26 Find the Percentage of Shipable Orders
# https://platform.stratascratch.com/coding/10090-find-the-percentage-of-shipable-orders?code_type=2

# Find the percentage of shipable orders.
# Consider an order is shipable if the customer's address is known.


# Python
# ******
import pandas as pd

df = orders.merge(customers[["id", "address"]], left_on="cust_id", right_on="id")

pct_shipable = df[df["address"].notna()].shape[0] / df.shape[0] * 100


# MySQL
# *****
SELECT
    COUNT(address) / (SELECT COUNT(*) FROM orders) * 100 AS pct_shipable
FROM orders AS o
JOIN customers AS c
    ON o.cust_id = c.id
WHERE address IS NOT NULL;



# 2.27 Spam Posts
# https://platform.stratascratch.com/coding/10134-spam-posts?code_type=2

# Calculate the percentage of spam posts in all viewed posts by day.
# A post is considered a spam if a string "spam" is inside keywords of the post.
# Note that the facebook_posts table stores all posts posted by users.
# The facebook_post_views table is an action table denoting if a user has viewed a post.


# Python
# ******
import pandas as pd

df = facebook_posts[facebook_posts["post_id"].isin(facebook_post_views["post_id"])].assign(spam_pctg = 0)
df["spam_pctg"] = df["spam_pctg"].mask(df["post_keywords"].str.contains("spam", case=False, regex=False), 100)

df_gr = df.groupby(by="post_date", as_index=False)["spam_pctg"].mean()


# MySQL
# *****
WITH cte_spam AS
(
SELECT DISTINCT
    p.post_id,
    post_date,
    CASE WHEN LOWER(post_keywords) LIKE '%spam%' THEN 100 ELSE 0 END AS spam_value
FROM facebook_posts AS p
JOIN facebook_post_views AS v
    USING (post_id)
)
SELECT
    post_date,
    AVG(spam_value) AS spam_pctg
FROM cte_spam
GROUP BY post_date
ORDER BY post_date;



# 2.28 Apple Product Counts
# https://platform.stratascratch.com/coding/10141-apple-product-counts?code_type=2

# We’re analyzing user data to understand how popular Apple devices are among users who have performed at least one event on the platform.
# Specifically, we want to measure this popularity across different languages.
# Count the number of distinct users using Apple devices —limited to "macbook pro", "iphone 5s", and "ipad air" —
# and compare it to the total number of users per language.
# Present the results with the language, the number of Apple users, and the total number of users for each language.
# Finally, sort the results so that languages with the highest total user count appear first.


# Python
# ******
import pandas as pd

apple_device_list = ["macbook pro", "iphone 5s", "ipad air"]

playbook_events = playbook_events.assign(apple_device = 0)
playbook_events["apple_device"] = playbook_events["apple_device"].mask(playbook_events["device"].isin(apple_device_list), 1)

df = playbook_events.merge(playbook_users, how="left", on="user_id")[["user_id", "apple_device", "language"]].drop_duplicates().sort_values(by="user_id")

df_gr = df.groupby(by="language", as_index=False).agg(
    apple_users_count = ("apple_device", "sum"),
    total_users_count = ("user_id", "nunique")
    ).sort_values(by="total_users_count", ascending=False)


# MySQL
# *****
WITH cte_merged AS
(
SELECT DISTINCT
    e.user_id,
    CASE WHEN device IN ('macbook pro', 'iphone 5s', 'ipad air') THEN 1 ELSE 0 END AS apple_device,
    language
FROM playbook_events AS e
LEFT JOIN playbook_users AS u
    USING (user_id)
ORDER BY user_id
)
SELECT
    language,
    SUM(apple_device) AS apple_users_count,
    COUNT(DISTINCT user_id) AS total_users_count
FROM cte_merged
GROUP BY language
ORDER BY total_users_count DESC;



# 2.29 No Order Customers
# https://platform.stratascratch.com/coding/10142-no-order-customers?code_type=2

# Identify customers who did not place an order between 2019-02-01 and 2019-03-01.
# Include:
# - Customers who placed orders only outside this date range.
# - Customers who never placed any orders.
# Output the customers' first names.


# Python
# ******
# Solution #1 - Simple
import pandas as pd

df_inside = orders[orders["order_date"].between("2019-02-01", "2019-03-01")]["cust_id"].drop_duplicates()

df_mrg = customers.merge(df_inside, how="left", left_on="id", right_on="cust_id")

df_outside_or_never = df_mrg[df_mrg["cust_id"].isna()][["first_name"]]


# Solution #2 - More complicated
import pandas as pd

df_mrg = customers.merge(orders, how="left", left_on="id", right_on="cust_id")[["id_x", "first_name", "order_date"]]

df_inside = df_mrg[df_mrg["order_date"].between("2019-02-01", "2019-03-01")]

df_outside_or_never = df_mrg[~df_mrg["id_x"].isin(df_inside["id_x"])][["id_x", "first_name"]].drop_duplicates()[["first_name"]]


# MySQL
# *****
WITH cte_inside AS
(
SELECT DISTINCT cust_id
FROM orders
WHERE order_date BETWEEN '2019-02-01' AND '2019-03-01'
)
SELECT first_name
FROM customers
LEFT JOIN cte_inside
    ON id = cust_id
WHERE cust_id IS NULL;



# 2.30 Number Of Units Per Nationality
# https://platform.stratascratch.com/coding/10156-number-of-units-per-nationality?code_type=2

# We have data on rental properties and their owners.
# Write a query that figures out how many different apartments (use unit_id) are owned by people under 30, broken down by their nationality.
# We want to see which nationality owns the most apartments, so make sure to sort the results accordingly.


# Python
# ******
import pandas as pd

df = airbnb_units.merge(airbnb_hosts.drop_duplicates(), how="left", on="host_id")[["unit_id", "unit_type", "nationality", "age"]]

df_gr = df[
    df["age"].lt(30) & 
    df["unit_type"].eq("Apartment")
    ].groupby(by="nationality")["unit_id"].nunique().to_frame("apartment_count").reset_index().sort_values(by="apartment_count", ascending=False)


# MySQL
# *****
SELECT
    nationality,
    COUNT(DISTINCT unit_id) AS apartment_count
FROM airbnb_units
LEFT JOIN (SELECT DISTINCT * FROM airbnb_hosts) AS h
    USING (host_id)
WHERE
    age < 30 AND
    unit_type = 'Apartment'
GROUP BY nationality
ORDER BY apartment_count DESC;



# 2.31 Ranking Most Active Guests
# https://platform.stratascratch.com/coding/10159-ranking-most-active-guests?code_type=2

# Identify the most engaged guests by ranking them according to their overall messaging activity.
# The most active guest, meaning the one who has exchanged the most messages with hosts, should have the highest rank.
# If two or more guests have the same number of messages, they should have the same rank.
# Importantly, the ranking shouldn't skip any numbers, even if many guests share the same rank.
# Present your results in a clear format, showing the rank, guest identifier, and total number of messages for each guest,
# ordered from the most to least active.


# Python
# ******
import pandas as pd

df_gr = airbnb_contacts.groupby(by="id_guest")["n_messages"].sum().to_frame("n_messages_sum").reset_index()

df_gr = df_gr.assign(ranking = df_gr["n_messages_sum"].rank(method="dense", ascending=False)).sort_values(by=["ranking", "id_guest"])


# MySQL
# *****
SELECT
    id_guest,
    SUM(n_messages) AS n_messages_sum,
    DENSE_RANK() OVER(ORDER BY SUM(n_messages) DESC) AS ranking
FROM airbnb_contacts
GROUP BY id_guest
ORDER BY ranking, id_guest;



# 2.32 Number of Streets Per Zip Code
# https://platform.stratascratch.com/coding/10182-number-of-streets-per-zip-code?code_type=2

# Count the number of unique street names for each postal code in the business dataset.
# Use only the first word of the street name, case insensitive (e.g., "FOLSOM" and "Folsom" are the same).
# If the structure is reversed (e.g., "Pier 39" and "39 Pier"), count them as the same street.
# Output the results with postal codes, ordered by the number of streets (descending) and postal code (ascending).


# Python
# ******
import pandas as pd

df = sf_restaurant_health_violations[sf_restaurant_health_violations["business_postal_code"].notna()][["business_postal_code", "business_address"]]

df = df.assign(street_1st_word = df["business_address"].str.lower().str.replace("'", "").str.extract(r"(?:^\s*\d+[\w\-]*\s+)?(\b\w+\b)"))

df_gr = df.groupby(by="business_postal_code")["street_1st_word"].nunique().to_frame("streets_count").reset_index()
df_gr.sort_values(by=["streets_count", "business_postal_code"], ascending=[False, True])


# MySQL
# *****
WITH cte_address_clean AS
(
SELECT
    business_postal_code,
    LOWER( REPLACE(business_address, "'", "") ) AS address_clean
FROM sf_restaurant_health_violations
WHERE business_postal_code IS NOT NULL
),
cte_street_1st_word AS
(
SELECT
    business_postal_code,
    SUBSTRING_INDEX( TRIM( REGEXP_REPLACE(address_clean, '^\\s*\\d+[\\w-]*\\s+', '') ), ' ', 1 ) AS street_1st_word
FROM cte_address_clean
)
SELECT
    business_postal_code,
    COUNT(DISTINCT street_1st_word) AS streets_count
FROM cte_street_1st_word
GROUP BY business_postal_code
ORDER BY streets_count DESC, business_postal_code;
