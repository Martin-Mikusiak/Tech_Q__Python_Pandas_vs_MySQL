# StrataScratch - Technical Questions (only the "Free" ones)
# **********************************************************
# https://platform.stratascratch.com/coding?is_freemium=1&code_type=2&page_size=100&difficulties=3

# My own solutions - both in Python Pandas and in MySQL

# ***** In progress *****


# Contents

# 1. Difficulty: Easy  (27 Questions)
# --> See the previous file "Tech_Q__StrataScratch_1_Easy.py"


# 2. Difficulty: Medium  (41 Questions)
# --> See the previous file "Tech_Q__StrataScratch_2_Medium.py"


# 3. Difficulty: Hard  (13 Questions)
#    3.1 Marketing Campaign Success [Advanced]
#    3.2 Most Popular Client For Calls
#    3.3 Retention Rate
#    3.4 Cookbook Recipes
#    3.5 Host Popularity Rental Prices
#    3.6 City With Most Amenities
#    3.7 Counting Instances in Text
#    3.8 Top 5 States With 5 Star Businesses
#    3.9 Popularity Percentage
#    3.10 Top Percentile Fraud
#    3.11 Monthly Percentage Difference
#    3.12 Rank Variance Per Country
#    3.13 Consecutive Days
#    3.14 Player with Longest Streak
#    3.15 ***** In progress *****



# 3. Difficulty: Hard  (15 Questions)
# ***********************************

# 3.1 Marketing Campaign Success [Advanced]
# https://platform.stratascratch.com/coding/514-marketing-campaign-success-advanced?code_type=2

# You have the marketing_campaign table, which records in-app purchases by users.
# Users making their first in-app purchase enter a marketing campaign, where they see call-to-actions for more purchases.
# Find how many users made additional purchases due to the campaign's success.
# The campaign starts one day after the first purchase.
# Users with only one or multiple purchases on the first day do not count, nor do users who later buy only the same products from their first day.


# Python
# ******
# Solution #1 - Using Self-Merge of the DataFrame with the Dense Rank of the date column per each user
import pandas as pd

df = marketing_campaign.sort_values(by=["user_id", "created_at", "product_id"]).drop(columns=["quantity", "price"])
df = df.assign(date_d_r = df.groupby(by="user_id")["created_at"].rank(method="dense"))

df_mrg = df[df["date_d_r"].gt(1)].merge(df[df["date_d_r"].eq(1)], how="left", on=["user_id", "product_id"])

users_count = df_mrg[df_mrg["created_at_y"].isna()]["user_id"].nunique()


# Solution #2 - Using 2x Dense rank of the date column - each of them with different .groupby()
import pandas as pd

df = marketing_campaign.sort_values(by=["user_id", "created_at", "product_id"]).drop(columns=["quantity", "price"])
df = df.assign(date_d_r = df.groupby(by="user_id")["created_at"].rank(method="dense"))
df = df.assign(prod_d_r = df.groupby(by=["user_id", "product_id"])["created_at"].rank(method="dense"))

users_count = df[df["date_d_r"].gt(1) & df["prod_d_r"].eq(1)]["user_id"].nunique()


# MySQL
# *****
# Solution #1 - Using Self-Join of the CTE with the Dense Rank of the date column per each user
WITH cte_date_d_r AS
(
SELECT
    user_id,
    product_id,
    DENSE_RANK() OVER(PARTITION BY user_id ORDER BY created_at) AS date_d_r
FROM marketing_campaign
)
SELECT COUNT(DISTINCT nd.user_id) AS users_count
FROM      cte_date_d_r AS nd
LEFT JOIN cte_date_d_r AS fd
    ON nd.user_id = fd.user_id AND
    nd.product_id = fd.product_id AND
    nd.date_d_r > 1 AND
    fd.date_d_r = 1
WHERE
    nd.date_d_r > 1 AND
    fd.product_id IS NULL;


# Solution #2 - Using 2x DENSE_RANK() of the date column - each of them with different partitions
WITH cte_dense_ranks AS
(
SELECT
    user_id,
    created_at,
    product_id,
    DENSE_RANK() OVER(PARTITION BY user_id             ORDER BY created_at) AS date_d_r,
    DENSE_RANK() OVER(PARTITION BY user_id, product_id ORDER BY created_at) AS prod_d_r
FROM marketing_campaign
)
SELECT COUNT(DISTINCT user_id) AS users_count
FROM cte_dense_ranks
WHERE
    date_d_r > 1 AND
    prod_d_r = 1;



# 3.2 Most Popular Client For Calls
# https://platform.stratascratch.com/coding/2029-the-most-popular-client_id-among-users-using-video-and-voice-calls?code_type=2

# Select the most popular client_id based on the number of users who individually have at least 50% of their events from the following list:
# 'video call received', 'video call sent', 'voice call received', 'voice call sent'.


# Python
# ******
import pandas as pd

fact_events = fact_events.assign(call_pctg = 0)
fact_events["call_pctg"] = fact_events["call_pctg"].mask(fact_events["event_type"].str.contains("call"), 100)

df_gr = fact_events.groupby(by="user_id", as_index=False)["call_pctg"].mean()

df_mrg = df_gr[df_gr["call_pctg"].ge(50)].merge(fact_events, on="user_id").groupby(by="client_id").size().to_frame("client_count").reset_index()
df_mrg.nlargest(1, "client_count", keep="all")["client_id"]


# MySQL
# *****
WITH cte_call_pctg AS
(
SELECT
    user_id,
    AVG( CASE WHEN event_type LIKE '%call%' THEN 100 ELSE 0 END ) AS call_pctg
FROM fact_events
GROUP BY user_id
HAVING call_pctg >= 50
),
cte_client_count AS
(
SELECT
    client_id,
    COUNT(*) AS client_count
FROM cte_call_pctg
JOIN fact_events
    USING (user_id)
GROUP BY client_id
)
SELECT client_id
FROM cte_client_count
WHERE client_count = (SELECT MAX(client_count) FROM cte_client_count);



# 3.3 Retention Rate
# https://platform.stratascratch.com/coding/2053-retention-rate?code_type=2

# You are given a dataset that tracks user activity.
# The dataset includes information about the date of user activity, the account_id associated with the activity, and the user_id of the user performing the activity.
# Each row in the dataset represents a user’s activity on a specific date for a particular account_id.
# Your task is to calculate the monthly retention rate for users for each account_id for December 2020 and January 2021.
# The retention rate is defined as the percentage of users active in a given month who have activity in any future month.
# For instance, a user is considered retained for December 2020 if they have activity in December 2020 and any subsequent month (e.g., January 2021 or later).
# Similarly, a user is retained for January 2021 if they have activity in January 2021 and any later month (e.g., February 2021 or later).
# The final output should include the account_id and the ratio of the retention rate in January 2021 to the retention rate in December 2020 for each account_id.
# If there are no users retained in December 2020, the retention rate ratio should be set to 0.


# Python
# ******
import pandas as pd

sf_events = sf_events.assign(yyyymm = sf_events["record_date"].dt.strftime("%Y%m"))

df_202012 = sf_events[sf_events["yyyymm"].eq("202012")][["account_id", "user_id"]].drop_duplicates().rename(columns={"user_id": "uid_202012"})
df_202101 = sf_events[sf_events["yyyymm"].eq("202101")][["account_id", "user_id"]].drop_duplicates().rename(columns={"user_id": "uid_202101"})

df_202012_ret = sf_events[sf_events["record_date"].dt.year.ge(2021)][["account_id", "user_id"]].drop_duplicates().rename(columns={"user_id": "uid_202012_ret"})
df_202101_ret = sf_events[sf_events["record_date"].ge("2021-02-01")][["account_id", "user_id"]].drop_duplicates().rename(columns={"user_id": "uid_202101_ret"})

df_mrg_202012 = df_202012.merge(df_202012_ret, how="left", left_on=["account_id", "uid_202012"], right_on=["account_id", "uid_202012_ret"]).assign(ret_pctg_202012 = 100)
df_mrg_202012["ret_pctg_202012"] = df_mrg_202012["ret_pctg_202012"].mask(df_mrg_202012["uid_202012_ret"].isna(), 0)

df_mrg_202101 = df_202101.merge(df_202101_ret, how="left", left_on=["account_id", "uid_202101"], right_on=["account_id", "uid_202101_ret"]).assign(ret_pctg_202101 = 100)
df_mrg_202101["ret_pctg_202101"] = df_mrg_202101["ret_pctg_202101"].mask(df_mrg_202101["uid_202101_ret"].isna(), 0)

df_gr_202012 = df_mrg_202012.groupby(by="account_id", as_index=False)["ret_pctg_202012"].mean()
df_gr_202101 = df_mrg_202101.groupby(by="account_id", as_index=False)["ret_pctg_202101"].mean()

df_gr_ratio = df_gr_202012.merge(df_gr_202101, on="account_id").sort_values(by="account_id")
df_gr_ratio = df_gr_ratio.assign(retention_ratio = df_gr_ratio.apply(lambda row: row["ret_pctg_202101"] / row["ret_pctg_202012"] if row["ret_pctg_202012"] != 0 else 0, axis=1))
df_gr_ratio[["account_id", "retention_ratio"]]


# MySQL
# *****
WITH cte_202012 AS
(
SELECT DISTINCT
    account_id,
    user_id AS uid_202012
FROM sf_events
WHERE EXTRACT(YEAR_MONTH FROM record_date) = 202012
),
cte_202101 AS
(
SELECT DISTINCT
    account_id,
    user_id AS uid_202101
FROM sf_events
WHERE EXTRACT(YEAR_MONTH FROM record_date) = 202101
),
cte_202012_ret AS
(
SELECT DISTINCT
    account_id,
    user_id AS uid_202012_ret
FROM sf_events
WHERE EXTRACT(YEAR_MONTH FROM record_date) > 202012
),
cte_202101_ret AS
(
SELECT DISTINCT
    account_id,
    user_id AS uid_202101_ret
FROM sf_events
WHERE EXTRACT(YEAR_MONTH FROM record_date) > 202101
),
cte_202012_gr AS
(
SELECT
    c.account_id,
    AVG( CASE WHEN uid_202012_ret IS NOT NULL THEN 100 ELSE 0 END ) AS ret_pctg_202012
FROM cte_202012 AS c
LEFT JOIN cte_202012_ret AS r
    ON c.account_id = r.account_id AND
    uid_202012 = uid_202012_ret
GROUP BY c.account_id
),
cte_202101_gr AS
(
SELECT
    c.account_id,
    AVG( CASE WHEN uid_202101_ret IS NOT NULL THEN 100 ELSE 0 END ) AS ret_pctg_202101
FROM cte_202101 AS c
LEFT JOIN cte_202101_ret AS r
    ON c.account_id = r.account_id AND
    uid_202101 = uid_202101_ret
GROUP BY c.account_id
)
SELECT
    account_id,
    IF(ret_pctg_202012 = 0, 0, ret_pctg_202101 / ret_pctg_202012) AS retention_ratio
FROM cte_202012_gr
JOIN cte_202101_gr
    USING (account_id)
ORDER BY account_id;



# 3.4 Cookbook Recipes
# https://platform.stratascratch.com/coding/2089-cookbook-recipes?code_type=2

# You are given a table containing recipe titles and their corresponding page numbers from a cookbook.
# Your task is to format the data to represent how recipes are distributed across double-page spreads in the book.
# Each spread consists of two pages:
# - The left page (even-numbered) and its corresponding recipe title (if any).
# - The right page (odd-numbered) and its corresponding recipe title (if any).
# The output table should contain the following three columns:
# - left_page_number – The even-numbered page that starts each double-page spread.
# - left_title  – The title of the recipe on the left page (if available).
# - right_title – The title of the recipe on the right page (if available).
# For the k-th row (starting from 0):
# - The left_page_number should be 2 * k.
# - The left_title should be the title from page 2 * k, or NULL if there is no recipe on that page.
# - The right_title should be the title from page 2 * k + 1, or NULL if there is no recipe on that page.
# Each page contains at most one recipe and if a page does not contain a recipe, the corresponding title should be NULL.
# Page 0 (the inside cover) is always empty and included in the output.
# The table should ensure that all pages up to the maximum recorded page number are included.


# Python
# ******
import pandas as pd

p_n_max = cookbook_titles["page_number"].max()

if p_n_max % 2 == 1:
    p_n_max -= 1
    
df_cookbook = pd.DataFrame({
    "L_p_n": range(0, p_n_max + 1, 2),
    "R_p_n": range(1, p_n_max + 2, 2)
    })

df_cookbook = df_cookbook.merge(cookbook_titles, how="left", left_on="L_p_n", right_on="page_number")
df_cookbook = df_cookbook.rename(columns={"title": "L_title"}).drop(columns="page_number")

df_cookbook = df_cookbook.merge(cookbook_titles, how="left", left_on="R_p_n", right_on="page_number")
df_cookbook = df_cookbook.rename(columns={"title": "R_title"}).drop(columns=["R_p_n", "page_number"])


# MySQL
# *****
WITH RECURSIVE cte_numbers AS
(
SELECT 0 AS n
UNION ALL
SELECT n + 1 FROM cte_numbers
WHERE n + 1 <= (SELECT FLOOR( MAX(page_number) / 2 ) FROM cookbook_titles)
),
cte_pages AS
(
SELECT 
    n * 2     AS L_p_n,
    n * 2 + 1 AS R_p_n
FROM cte_numbers
)
SELECT 
    L_p_n,
    lt.title AS L_title,
    rt.title AS R_title
FROM cte_pages
LEFT JOIN cookbook_titles AS lt
    ON L_p_n = lt.page_number
LEFT JOIN cookbook_titles AS rt
    ON R_p_n = rt.page_number;



# 3.5 Host Popularity Rental Prices
# https://platform.stratascratch.com/coding/9632-host-popularity-rental-prices?code_type=2

# You are given a table named airbnb_host_searches that contains data for rental property searches made by users.
# Determine the minimum, average, and maximum rental prices for each popularity-rating bucket.
# A popularity-rating bucket should be assigned to every record based on its number_of_reviews (see rules below).
# The host’s popularity rating is defined as below:
# * 0 reviews:            "New"
# * 1 to 5 reviews:       "Rising"
# * 6 to 15 reviews:      "Trending Up"
# * 16 to 40 reviews:     "Popular"
# * More than 40 reviews: "Hot"
# Tip: The id column in the table refers to the search ID.
# Output host popularity rating and their minimum, average and maximum rental prices.
# Order the solution by the minimum price.


# Python
# ******
import pandas as pd

df = airbnb_host_searches[["price", "number_of_reviews"]].assign(host_popularity = "New")

df["host_popularity"] = df["host_popularity"].mask(df["number_of_reviews"].between( 1,  5), "Rising")
df["host_popularity"] = df["host_popularity"].mask(df["number_of_reviews"].between( 6, 15), "Trending Up")
df["host_popularity"] = df["host_popularity"].mask(df["number_of_reviews"].between(16, 40), "Popular")
df["host_popularity"] = df["host_popularity"].mask(df["number_of_reviews"].gt(40)         , "Hot")

df_gr = df.groupby(by="host_popularity", as_index=False).agg(
    min_price = ("price", "min" ),
    avg_price = ("price", "mean"),
    max_price = ("price", "max" )
    ).sort_values(by="min_price")


# MySQL
# *****
SELECT
    CASE
        WHEN number_of_reviews = 0               THEN 'New'
        WHEN number_of_reviews BETWEEN  1 AND  5 THEN 'Rising'
        WHEN number_of_reviews BETWEEN  6 AND 15 THEN 'Trending Up'
        WHEN number_of_reviews BETWEEN 16 AND 40 THEN 'Popular'
        WHEN number_of_reviews > 40              THEN 'Hot'
    END AS host_popularity,
    MIN(price) AS min_price,
    AVG(price) AS avg_price,
    MAX(price) AS max_price
FROM airbnb_host_searches
GROUP BY host_popularity
ORDER BY min_price;



# 3.6 City With Most Amenities
# https://platform.stratascratch.com/coding/9633-city-with-most-amenities?code_type=2

# You're given a dataset of searches for properties on Airbnb.
# For simplicity, let's say that each search result (i.e., each row) represents a unique host.
# Find the city with the most amenities across all their host's properties. Output the name of the city.


# Python
# ******
import pandas as pd

df = airbnb_search_details[["city", "amenities"]]
df = df.assign(amenities_count = df["amenities"].str.split(",").apply(len))
df["amenities_count"] = df["amenities_count"].mask(df["amenities"].eq("{}"), 0)

df_gr = df.groupby(by="city")["amenities_count"].sum().to_frame("amenities_sum").reset_index()
df_gr.nlargest(1, "amenities_sum", keep="all")["city"]


# MySQL
# *****
WITH cte_amenities_sum AS
(
SELECT 
    city,
    SUM(
        CASE
            WHEN amenities = '{}' THEN 0
            ELSE LENGTH(amenities) - LENGTH( REPLACE(amenities, ",", "") ) + 1
        END
    ) AS amenities_sum
FROM airbnb_search_details
GROUP BY city
)
SELECT city
FROM cte_amenities_sum
WHERE amenities_sum = (SELECT MAX(amenities_sum) FROM cte_amenities_sum);



# 3.7 Counting Instances in Text
# https://platform.stratascratch.com/coding/9814-counting-instances-in-text?code_type=2

# Find the number of times the exact words bull and bear appear in the contents column.
# Count all occurrences, even if they appear multiple times within the same row.
# Matches should be case-insensitive and only count exact words, that is, exclude substrings like bullish or bearing.
# Output the word (bull or bear) and the corresponding number of occurrences.


# Python
# ******
import pandas as pd

bull_count = google_file_store["contents"].str.lower().str.count(r"\bbull\b").sum()
bear_count = google_file_store["contents"].str.lower().str.count(r"\bbear\b").sum()

df = pd.DataFrame({
    "word" : ["bull", "bear"],
    "count": [bull_count, bear_count]
})


# MySQL
# *****
WITH cte_lwr_rmv_pnct AS
(
SELECT
    LOWER( REGEXP_REPLACE(contents, '[.,!?]', '') ) AS c_cleaned
FROM google_file_store
)
SELECT
    'bull' AS word,
    SUM( ( LENGTH(c_cleaned) - LENGTH( REGEXP_REPLACE(c_cleaned, '\\bbull\\b', '') ) ) / LENGTH('bull') ) AS word_count
FROM cte_lwr_rmv_pnct
UNION
SELECT
    'bear' AS word,
    SUM( ( LENGTH(c_cleaned) - LENGTH( REGEXP_REPLACE(c_cleaned, '\\bbear\\b', '') ) ) / LENGTH('bear') ) AS word_count
FROM cte_lwr_rmv_pnct;



# 3.8 Top 5 States With 5 Star Businesses
# https://platform.stratascratch.com/coding/10046-top-5-states-with-5-star-businesses?code_type=2

# Find the top 5 states with the most 5 star businesses.
# Output the state name along with the number of 5-star businesses and order records by the number of 5-star businesses in descending order.
# In case there are ties in the number of businesses, return all the unique states.
# If two states have the same result, sort them in alphabetical order.


# Python
# ******
import pandas as pd

df = yelp_business[yelp_business["stars"].eq(5)].groupby(by="state").size().to_frame("n_businesses").reset_index()
df = df.assign(d_rnk = df["n_businesses"].rank(method="dense", ascending=False))
df[df["d_rnk"].le(5)].sort_values(by=["d_rnk", "state"]).drop(columns="d_rnk")


# MySQL
# *****
WITH cte_d_rnk AS
(
SELECT
    state,
    COUNT(*) AS n_businesses,
    DENSE_RANK() OVER(ORDER BY COUNT(*) DESC) AS d_rnk
FROM yelp_business
WHERE stars = 5
GROUP BY state
ORDER BY d_rnk, state
)
SELECT
    state,
    n_businesses
FROM cte_d_rnk
WHERE d_rnk <= 5;



# 3.9 Popularity Percentage
# https://platform.stratascratch.com/coding/10284-popularity-percentage?code_type=2

# Find the popularity percentage for each user on Meta/Facebook.
# The dataset contains two columns, user1 and user2, which represent pairs of friends.
# Each row indicates a mutual friendship between user1 and user2, meaning both users are friends with each other.
# A user's popularity percentage is calculated as the total number of friends they have (counting connections from both user1 and user2 columns)
# divided by the total number of unique users on the platform.
# Multiply this value by 100 to express it as a percentage.
# Output each user along with their calculated popularity percentage.
# The results should be ordered by user ID in ascending order.


# Python
# ******
import pandas as pd

df = pd.concat([facebook_friends["user1"], facebook_friends["user2"]], ignore_index=True).to_frame("user")

df_gr = df.groupby(by="user").size().to_frame("friends_count").reset_index()
df_gr = df_gr.assign(popularity_pctg = df_gr["friends_count"] / df_gr.shape[0] * 100).drop(columns="friends_count")


# MySQL
# *****
WITH cte_users AS
(
SELECT user1 AS user
FROM facebook_friends
UNION ALL
SELECT user2 AS user
FROM facebook_friends
),
cte_friends_count AS
(
SELECT
    user,
    COUNT(*) AS friends_count
FROM cte_users
GROUP BY user
ORDER BY user
)
SELECT
    user,
    friends_count / (SELECT COUNT(*) FROM cte_friends_count) * 100 AS popularity_pctg
FROM cte_friends_count;



# 3.10 Top Percentile Fraud
# https://platform.stratascratch.com/coding/10303-top-percentile-fraud?code_type=2

# We want to identify the most suspicious claims in each state.
# We'll consider the top 5 percentile of claims with the highest fraud scores in each state as potentially fraudulent.
# Your output should include the policy number, state, claim cost, and fraud score.


# Python
# ******
import pandas as pd

fraud_score = fraud_score.assign(threshold = fraud_score.groupby(by="state")["fraud_score"].transform(lambda x: x.quantile(.95)))
fraud_score[fraud_score["fraud_score"].ge(fraud_score["threshold"])].drop(columns="threshold")


# MySQL
# *****
WITH cte_ntile_20 AS
(
SELECT
    state,
    fraud_score,
    NTILE(20) OVER(PARTITION BY state ORDER BY fraud_score DESC) AS ntile_20
FROM fraud_score
),
cte_threshold AS
(
SELECT
    state,
    MIN(fraud_score) AS threshold
FROM cte_ntile_20
WHERE ntile_20 = 1
GROUP BY state
)
SELECT f.*
FROM fraud_score AS f
JOIN cte_threshold AS t
    USING (state)
WHERE fraud_score >= threshold;

# Note: For the latest versions of MySQL there is:  SELECT ..., PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fraud_score) OVER (PARTITION BY state) AS threshold



# 3.11 Monthly Percentage Difference
# https://platform.stratascratch.com/coding/10319-monthly-percentage-difference?code_type=2

# Given a table of purchases by date, calculate the month-over-month percentage change in revenue.
# The output should include the year-month date (YYYY-MM) and percentage change, rounded to the 2nd decimal point,
# and sorted from the beginning of the year to the end of the year.
# The percentage change column will be populated from the 2nd month forward and can be calculated as:
# ((this month's revenue - last month's revenue) / last month's revenue) * 100


# Python
# ******
import pandas as pd

sf_transactions = sf_transactions.assign(yyyymm = sf_transactions["created_at"].dt.strftime("%Y-%m"))

df_gr = sf_transactions.groupby("yyyymm", as_index=False).agg(revenue = ("value", "sum"))
df_gr = df_gr.assign(rev_pctg_chg = (df_gr["revenue"].diff() / df_gr["revenue"].shift(1) * 100).round(2)).drop(columns="revenue")


# MySQL
# *****
WITH cte_revenue AS
(
SELECT
    DATE_FORMAT(created_at, '%Y-%m') AS yyyymm,
    SUM(value) AS revenue
FROM sf_transactions
GROUP BY yyyymm
ORDER BY yyyymm
)
SELECT
    yyyymm,
    ROUND( (revenue - LAG(revenue) OVER()) / LAG(revenue) OVER() * 100, 2 ) AS rev_pctg_chg
FROM cte_revenue;



# 3.12 Rank Variance Per Country
# https://platform.stratascratch.com/coding/2007-rank-variance-per-country?code_type=2

# Which countries have risen in the rankings based on the number of comments between Dec 2019 vs Jan 2020?
# Hint: Avoid gaps between ranks when ranking countries.


# Python
# ******
import pandas as pd

fb_comments_count = fb_comments_count.assign(yyyymm = fb_comments_count["created_at"].dt.strftime("%Y%m"))

df_cc = fb_comments_count.merge(fb_active_users, on="user_id")

df_cc_pt = df_cc.pivot_table(index="country", columns="yyyymm", values="number_of_comments", aggfunc="sum", fill_value=0).reset_index()[["country", "201912", "202001"]]

df_cc_pt = df_cc_pt.assign(rnk_201912 = df_cc_pt["201912"].rank(method="dense", ascending=False))
df_cc_pt = df_cc_pt.assign(rnk_202001 = df_cc_pt["202001"].rank(method="dense", ascending=False))

df_cc_pt[df_cc_pt["rnk_201912"].gt(df_cc_pt["rnk_202001"])][["country"]]


# MySQL
# *****
WITH cte_joined AS
(
SELECT
    country,
    SUM( CASE WHEN EXTRACT(YEAR_MONTH FROM created_at) = '201912' THEN number_of_comments END ) AS 201912_c,
    SUM( CASE WHEN EXTRACT(YEAR_MONTH FROM created_at) = '202001' THEN number_of_comments END ) AS 202001_c
FROM fb_comments_count
JOIN fb_active_users
    USING (user_id)
GROUP BY country
),
cte_d_rnk AS
(
SELECT
    country,
    DENSE_RANK() OVER(ORDER BY 201912_c DESC) AS rnk_201912,
    DENSE_RANK() OVER(ORDER BY 202001_c DESC) AS rnk_202001
FROM cte_joined
)
SELECT country
FROM cte_d_rnk
WHERE rnk_201912 > rnk_202001;



# 3.13 Consecutive Days
# https://platform.stratascratch.com/coding/2054-consecutive-days?code_type=2

# Find all the users who were active for 3 consecutive days or more.


# Python
# ******
import pandas as pd

sf_events = sf_events.sort_values(by=["user_id", "record_date"]).drop(columns="account_id")
sf_events = sf_events.assign(date_diff = sf_events.groupby(by="user_id")["record_date"].diff().dt.days)

df = sf_events[sf_events["date_diff"].eq(1) & sf_events["date_diff"].shift(1).eq(1)].drop_duplicates()[["user_id"]]


# MySQL
# *****
WITH cte_date_diff AS
(
SELECT
    user_id,
    record_date,
    DATEDIFF(record_date, LAG(record_date) OVER(PARTITION BY user_id ORDER BY record_date)) AS date_diff
FROM sf_events
),
cte_cnsctv_days AS
(
SELECT
    user_id,
    CASE WHEN date_diff = 1 AND LAG(date_diff) OVER() = 1 THEN 1 END AS cnsctv_days
FROM cte_date_diff
)
SELECT DISTINCT user_id
FROM cte_cnsctv_days
WHERE cnsctv_days = 1;



# 3.14 Player with Longest Streak
# https://platform.stratascratch.com/coding/2059-player-with-longest-streak?code_type=2

# You are given a table of tennis players and their matches that they could either win (W) or lose (L).
# Find the longest streak of wins. A streak is a set of consecutive won matches of one player.
# The streak ends once a player loses their next match.
# Output the ID of the player or players and the length of the streak.


# Python
# ******
import pandas as pd

df = players_results.sort_values(by=["player_id", "match_date"]).reset_index(drop=True)
df = df.assign(result = df["match_result"].eq("W").astype(int))

df = df.assign(r_diff = df.groupby(by="player_id")["result"].diff().fillna(df["result"]))

df = df.assign(grp_id = df[df["r_diff"].eq(1)].groupby(by="player_id")["r_diff"].cumsum())
df = df[df["result"].eq(1)]
df["grp_id"] = df["grp_id"].ffill()

df_gr = df.groupby(by=["player_id", "grp_id"], as_index=False).agg(streak_length = ("result", "count"))
df_gr[df_gr["streak_length"].eq(df_gr["streak_length"].max())][["player_id", "streak_length"]].drop_duplicates()


# MySQL
# *****
WITH cte_matches AS
(
SELECT 
    player_id,
    match_date,
    CASE WHEN match_result = 'W' THEN 1 ELSE 0 END AS result,
    ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY match_date) AS r_n
FROM players_results
),
cte_streaks_grp AS
(
SELECT 
    player_id,
    result,
    r_n - SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY player_id ORDER BY match_date) AS grp_id
FROM cte_matches
),
cte_streak_length AS
(
SELECT
    player_id,
    COUNT(*) AS streak_length
FROM cte_streaks_grp
WHERE result = 1
GROUP BY player_id, grp_id
)
SELECT DISTINCT
    player_id,
    streak_length
FROM cte_streak_length
WHERE streak_length = (SELECT MAX(streak_length) FROM cte_streak_length);
