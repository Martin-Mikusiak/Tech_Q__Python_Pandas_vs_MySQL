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


# 3. Difficulty: Hard  (12 Questions)
#    3.1 Marketing Campaign Success [Advanced]
#    3.2 Most Popular Client For Calls
#    3.3 Retention Rate
#    3.4 Cookbook Recipes
#    3.5 Host Popularity Rental Prices
#    3.6 City With Most Amenities
#    3.7 Counting Instances in Text
#    3.8 ***** In progress *****
#    3.9 
#    3.10 
#    3.11 
#    3.12 



# 3. Difficulty: Hard  (12 Questions)
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
