#!/bin/bash

SET accounting_group analytics-internal-processing-dev;
SET min_completion_ratio 1;
SET io_timeout 2400;
SET nest_join_schema true;
SET runtime_name dremel;
SET materialize_overwrite true;
SET materialize_owner_group analytics-internal-processing-dev;
SET nest_join_schema False;
SET dat_policy dremel;

MATERIALIZE '/cns/yi-d/home/aredakov/app_annie/apps_usage_months/data' AS
SELECT
  Package_Name AS package_name,
  LEFT(Period,7) AS year_month,
  SUM(weekly_time) AS sum_min_weekly,
  AVG(weekly_users) AS avg_users_weekly
FROM
 (SELECT
    Period,
    Package_Name,
    INT32(EXTRACT_REGEXP(Avg_Time_Per_User, "[^:]*:[^:]*:([^:]*)")) / 60 +
    INT32(EXTRACT_REGEXP(Avg_Time_Per_User, "[^:]*:([^:]*):[^:]*")) +
    INT32(EXTRACT_REGEXP(Avg_Time_Per_User, "([^:]*):[^:]*:[^:]*")) * 60 AS weekly_time,
    Active_Users AS weekly_users
  FROM android_cret_playfull.production.apps.app_annie.android.usage.all
  WHERE Rank_Category = 'Overall'
    AND Country = 'Worldwide (excluding China)'
    AND Device = 'Android'
  GROUP@50 BY 1,2,3,4)
GROUP@50 BY 1,2;

# categories.
MATERIALIZE '/cns/yi-d/home/aredakov/cleaned_apps/apps_categories/data' AS
SELECT
  package_name,
  CASE
    WHEN category IN (30, 13, 8, 18, 35) THEN 'books_libraries'
    WHEN category IN (5, 15, 59) THEN 'socail_communications'
    WHEN category IN (6, 60) THEN 'entertainment'
    WHEN category IN (19,22,23) THEN 'medical'
    WHEN category IN (62,9,63,20,58,56) THEN 'lifestyle_sport'
    WHEN category IN (11,27) THEN 'news'
    WHEN category IN (17,33) THEN 'travel'
    WHEN category IN (66, 32, 12, 21, 16,28) THEN 'utilities'
    WHEN category IN (24, 10, 25, 65, 26) THEN 'media_and_multimedia'
    WHEN category IN (38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
      52, 53, 54, 55) THEN 'games'
    WHEN category = 57 THEN 'auto_and_vehicles'
    WHEN category = 29 THEN 'business'
    WHEN category = 31 THEN 'education'
    WHEN category = 7 THEN  'finance'
    WHEN category = 61 THEN 'food_and_drink'
    WHEN category = 14 THEN 'shopping'
  ELSE 'Other'
  END AS category_name
FROM play_analytics.apps_data.all
GROUP@50 BY 1,2;

DEFINE TABLE apps_usage_months /cns/yi-d/home/aredakov/app_annie/apps_usage_months/data*;
DEFINE TABLE apps_categories /cns/yi-d/home/aredakov/cleaned_apps/apps_categories/data*;
# MATERIALIZE '/cns/ig-d/home/aredakov/wg_distrib/apps_categories_stats/data' AS
SELECT
  category_name,
  LEFT(year_month,4) AS q1_year,
  SUM(sum_min_weekly) AS total_time_in_app,
  AVG(avg_users_weekly) AS mean_weekly_users
FROM apps_usage_months a
LEFT JOIN@50 apps_categories b
ON a.package_name = b.package_name
WHERE year_month CONTAINS '-01'
  OR year_month CONTAINS '-02'
  OR year_month CONTAINS '-03'
GROUP@50 BY 1,2;
