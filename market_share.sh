#!/bin/bash
#

source "$( dirname $0 )/upload_metric.sh"
source "$( dirname $0 )/util.sh"

set -x

function crome_ga_hosts () {

  local country="$1"
  local partitions=50
  local rprefix="$2"
  local date="$3"
  local vertical="$4"
  local gdrive_file="${country}_${vertical}_${date}"
  local hits_to_visits_rate=0.9

  tee /dev/stderr << EOF | dremel
  ${DREMEL_INIT}

  MATERIALIZE '${rprefix}/ga_coverage/${country}_${vertical}/top_biz_domains/data@1' AS
  SELECT domain,
    CASE
        WHEN vertical CONTAINS ('Finance') THEN 'Finance'
        WHEN vertical CONTAINS ('Shopping/') OR vertical == '/Shopping' THEN 'Shopping'
        WHEN vertical CONTAINS ('Travel') THEN 'Travel'
        WHEN vertical CONTAINS ('Autos') THEN 'Autos'
        WHEN vertical CONTAINS ('Business & Industrial') THEN 'Business'
        ELSE vertical
    END AS vertical,
    ROUND(visits) AS visits
  FROM
    (SELECT destination_dims.domain AS domain, destination_dims.vertical AS vertical,
      SUM(total_visits_population_estimate) AS visits
    FROM bunyan.ads_quality_querynav.DailyPopulationEstimateValueScapeStats
    WHERE user_country == '${country}'
      AND date > STRFTIME_USEC(DATE_ADD(NOW(),-60,'DAY'),'%Y-%m-%d')
      AND destination_dims.domain
      NOT IN ('too_few_visits.domain','anonymized.domain','undiscovered domain')
      AND destination_dims.vertical LIKE '/${vertical}%'
    GROUP@${partitions} BY 1,2)
  WHERE visits > 1000;

  DEFINE TABLE hostnames ${rprefix}/ga_coverage/hostnames_hits_be_config/data*;
  DEFINE TABLE verticals_domains ${rprefix}/ga_coverage/${country}_${vertical}/top_biz_domains/data*;
  MATERIALIZE 'trix:/gdrive/aredakov/${country}_${vertical}/${gdrive_file}@1 header:true' AS
  SELECT crome_domains,
    CASE
      WHEN ratio < ${hits_to_visits_rate} OR ratio IS NULL THEN ""
      WHEN ratio >= ${hits_to_visits_rate} THEN "in_ga"
    END status,
    ratio, hits_1_day, visits_60_day
  FROM
    (SELECT domain AS crome_domains, hits AS hits_1_day,
      MAX(visits) AS visits_60_day, ROUND(MAX(hits/(visits/60)),2) AS ratio
    FROM verticals_domains  v
    LEFT JOIN@50 hostnames h
    ON domain = host
    GROUP@${partitions} BY 1,2)
  ORDER BY 5 DESC;

  MATERIALIZE 'csv:${rprefix}/ga_coverage/${country}_${vertical}_sample_size/data.csv@1' AS
  SELECT COUNT(DISTINCT domain) AS sample_size
  FROM verticals_domains;

EOF

  local sample_size="$( fileutil cat "${rprefix}/ga_coverage/${country}_${vertical}_sample_size/data.csv-00000-of-00001" )"

  tee /dev/stderr << EOF | dremel
  ${DREMEL_INIT}

  DEFINE TABLE hostnames ${rprefix}/ga_coverage/hostnames_hits_be_config/data*;
  DEFINE TABLE verticals_domains ${rprefix}/ga_coverage/${country}_${vertical}/top_biz_domains/data*;
  MATERIALIZE 'csv:${rprefix}/ga_coverage/${country}_count_by_verticals/data.csv@1' AS
  SELECT vertical, (COUNT(DISTINCT host))/${sample_size} AS share_covered
  FROM
    (SELECT vertical, host, hits, MAX(hits/(visits/30)) AS share,
      MAX(visits) AS visits
    FROM verticals_domains  v
    LEFT JOIN@50 hostnames h
    ON domain = host
    GROUP@${partitions} BY 1,2,3)
  WHERE share >= ${hits_to_visits_rate}
  GROUP BY 1;

EOF

  fileutil cat "${rprefix}/ga_coverage/${country}_count_by_verticals/data.csv-00000-of-00001" | grep "${vertical}" | while read line; do
    local segment=$( echo "${line}"  | cut -d ',' -f 1 )
    local value=$( echo "${line}" | cut -d ',' -f 2 )
    local csv_data_filename="${rprefix}/ga_coverage/${country}_count_by_verticals/data.csv-00000-of-00001"
    local tmpfile=$( mktemp )
    echo "${value}" > "${tmpfile}"
    upload_metric \
        --date "${date}" \
        --docs_url "${gdrive_url}" \
        --type 'GA Share' Totals "Coverage_${country}_${segment}" "${tmpfile}" \
        --create_gdrive_csv_from_file "${csv_data_filename}" \
    # rm "${tmpfile}"
  done
}

function market_share_ga () {

  local date="${1:-$( days_from_now 0 )}"
  local rprefix="/cns/ig-d/home/gacc/reports/rum_ga/${date}"
  local date_nodashes=$( echo "${date}" | sed 's/-//g' )
  local bigtable_date=$( /google/data/ro/projects/analytics/encode_helper \
  --print_aggregate_row_key \
  --profile_id=50331648 --table_id=23 \
  --date="$( echo "${date}" | sed 's/-//g' )" --index=0 |
  grep date | awk '{print $2}' )

   fileutil ls "${rprefix}/hits/shard0_all_hits_${date_nodashes}/data-00000-of-*" > /dev/null || \
  SAWMILL_ACCOUNTING_USER=urchin-processing-qa \
  /google/data/ro/projects/sqlmr/sqlmr \
  --query='
    SELECT INT32(EXTRACT_REGEXP(rowkey, "P:[^:]*:([^:]*):")) AS account_id,
      INT32(EXTRACT_REGEXP(rowkey, "P:[^:]*:[^:]*:([^:]*)")) AS log,
      SUM(INT32(counts_all.value)) AS sum_hits
    FROM bigtable("", "/bigtable/srv-iy-urchin/urchin-processing.processing_stats")
      OVER counts_all
    WHERE startswith(rowkey, "P:'"${date_nodashes}"':")
      AND counts_all.name = ""
    GROUP BY account_id,log' \
  --cell=iy \
  --ram=25G \
  --flume_main_java_heap=25G \
  --customMrFlags="--jmapreduce_buffer_max_size_bytes=2147483647" \
  --into "columnio('""${rprefix}/hits/shard0_all_hits_${date_nodashes}/data@*""')"

  fileutil ls "${rprefix}/hits/shard1_all_hits_${date_nodashes}/data-00000-of-*" > /dev/null || \
  SAWMILL_ACCOUNTING_USER=urchin-processing-qa \
  /google/data/ro/projects/sqlmr/sqlmr \
  --query='
    SELECT INT32(EXTRACT_REGEXP(rowkey, "P:[^:]*:([^:]*):")) AS account_id,
      INT32(EXTRACT_REGEXP(rowkey, "P:[^:]*:[^:]*:([^:]*)")) AS log,
      SUM(INT32(counts_all.value)) AS sum_hits
    FROM bigtable("", "/bigtable/srv-iy-urchin/urchin-processing.shard1.processing_stats")
      OVER counts_all
    WHERE startswith(rowkey, "P:'"${date_nodashes}"':")
      AND counts_all.name = ""
    GROUP BY account_id,log' \
  --cell=iy \
  --ram=25G \
  --flume_main_java_heap=25G \
  --customMrFlags="--jmapreduce_buffer_max_size_bytes=2147483647" \
  --into "columnio('""${rprefix}/hits/shard1_all_hits_${date_nodashes}/data@*""')"

 fileutil test -f "${rprefix}/ga_coverage/ga_hosts/data-00000-of-01000" || \
 SAWMILL_ACCOUNTING_USER=urchin-processing-qa \
  /google/data/ro/projects/sqlmr/sqlmr \
  --query='
    SELECT rowkey, aggregates.record.key.entry AS host_name,
     aggregates.record.value.pageviews AS pageviews
    FROM  bigtable("", "/bigtable/srv-iy-urchin/urchin-processing.aggregates")
      OVER aggregates.record.key.entry
    WHERE  rowkey CONTAINS "-M'"${bigtable_date}"'-"
      AND aggregates.record.key.entry != "(not set)"
      AND aggregates.record.key.entry != "localhost"
     GROUP BY rowkey, host_name, pageviews' \
  --cell=iy \
  --ram=25G \
  --flume_main_java_heap=25G \
  --customMrFlags="--jmapreduce_buffer_max_size_bytes=2147483647" \
  --into "columnio('${rprefix}/ga_coverage/ga_hosts/data@*')"

  tee /dev/stderr << EOF | dremel
  ${DREMEL_INIT}

  DEFINE TABLE shard0_all_hits ${rprefix}/shard0_all_hits_${date_nodashes}/data*;
  DEFINE TABLE shard1_all_hits ${rprefix}/shard1_all_hits_${date_nodashes}/data*;
  MATERIALIZE '${rprefix}/ga_coverage/hostnames_hits_config/data@1' AS
  SELECT DOMAIN(webPropertyProto.default_website_url) AS host, sum_hits
  FROM configstore_preprod.WebProperties w
  JOIN@100 shard0_all_hits h
    ON webPropertyProto.tracking_id.account_id = account_id
    AND webPropertyProto.tracking_id.log_number = log
  WHERE sum_hits >= 10000
    AND webPropertyProto.default_website_url NOT LIKE "%://m.%"
    AND webPropertyProto.default_website_url NOT LIKE "%://www.m.%"
    AND webPropertyProto.default_website_url NOT LIKE "%://mobile.%"
    AND webPropertyProto.default_website_url NOT LIKE "%://www.mobile.%"
    AND webPropertyProto.default_website_url NOT LIKE "%://app.%"
    AND webPropertyProto.default_website_url NOT LIKE "%://apps.%"
  GROUP@100 BY 1,2;

  DEFINE TABLE hosts_views ${rprefix}/ga_coverage/ga_hosts/data*;
  MATERIALIZE '${rprefix}/ga_coverage/max_hostnames/data@1' AS
  SELECT h.rowkey AS rowkey, pageviews, DOMAIN(host_name) AS host_name
  FROM hosts_views h
  JOIN@50
    (SELECT rowkey, MAX(pageviews) AS max_pageviews
    FROM hosts_views
    GROUP@100 BY 1) m
  ON h.rowkey = m.rowkey AND pageviews = max_pageviews
  WHERE pageviews >= 1000
    AND host_name NOT LIKE "m.%"
    AND host_name NOT LIKE "mobile.%"
    AND host_name NOT LIKE "%://m.%"
    AND host_name NOT LIKE "%://www.m.%"
    AND host_name NOT LIKE "%://mobile.%"
    AND host_name NOT LIKE "%://www.mobile.%"
    AND host_name NOT LIKE "%://app.%"
    AND host_name NOT LIKE "%://apps.%"
  GROUP@100 BY 1,2,3;

  DEFINE TABLE max_hostnames ${rprefix}/ga_coverage/max_hostnames/data*;

  MATERIALIZE '${rprefix}/ga_coverage/max_traffic_share/data@1' AS
  SELECT host_name, rowkey
  FROM
  (SELECT host_name, d.rowkey AS rowkey, ROUND((pageviews/sum_views)*100)
    AS traffic_share
  FROM max_hostnames d
  JOIN@50
      (SELECT rowkey, SUM(pageviews) AS sum_views
      FROM hosts_views
      GROUP@50 BY 1) s
  ON d.rowkey = s.rowkey)
  WHERE traffic_share > 90;

EOF

    blaze run -c opt //analytics/dashboards/datamart/decoder --  \
  --input=${rprefix}/ga_coverage/max_traffic_share/data* \
  --mapreduce_output_map=columnio:${rprefix}/ga_coverage/decoded_hostnames@100 \
  --decoders="rowkey:aggregate_row_key" \
  --mapreduce_borgcfg=priority=119,accounting_user=urchin-processing-qa

  tee /dev/stderr << EOF | dremel
  ${DREMEL_INIT}

  DEFINE TABLE decoded_hostnames ${rprefix}/ga_coverage/decoded_hostnames*;
  DEFINE TABLE all_hits ${rprefix}/all_hits_${date_nodashes}/data*;
  MATERIALIZE '${rprefix}/ga_coverage/host_account_log/data@1' AS
  SELECT webPropertyProto.tracking_id.account_id AS account_id,
     webPropertyProto.tracking_id.log_number AS log_number,
     host
  FROM
    (SELECT webPropertyId, host
    FROM configstore_preprod.Profiles p
    JOIN@50 decoded_hostnames d
    ON profileId = profile_id
    GROUP@50 BY 1,2) q
  JOIN@50 configstore_preprod.WebProperties w
  ON q.webPropertyId = w.webPropertyId
  WHERE webPropertyProto.default_website_url NOT LIKE "%://m.%"
    AND webPropertyProto.default_website_url NOT LIKE "%://www.m.%"
    AND webPropertyProto.default_website_url NOT LIKE "%://mobile.%"
    AND webPropertyProto.default_website_url NOT LIKE "%://www.mobile.%"
    AND webPropertyProto.default_website_url NOT LIKE "%://app.%"
    AND webPropertyProto.default_website_url NOT LIKE "%://apps.%"
  GROUP@50 BY 1,2,3;

  DEFINE TABLE host_account_log ${rprefix}/ga_coverage/host_account_log/data*;
  MATERIALIZE '${rprefix}/ga_coverage/hostnames_hits_gabe/data@1' AS
  SELECT host, sum_hits
  FROM host_account_log z
    JOIN@100 all_hits h
    ON z.account_id = h.account_id
    AND z.log_number = h.log
    WHERE sum_hits > 10000
  GROUP@100 BY 1,2;

  DEFINE TABLE hostnames_hits_gabe ${rprefix}/ga_coverage/hostnames_hits_gabe/data*;
  DEFINE TABLE hostnames_hits_config ${rprefix}/ga_coverage/hostnames_hits_config/data*;
  MATERIALIZE '${rprefix}/ga_coverage/hostnames_hits_be_config/data@1' AS
  SELECT host, MAX(sum_hits) AS sum_hits
  FROM hostnames_hits_gabe, hostnames_hits_config
  WHERE host NOT LIKE '%google%'
  GROUP@50 BY 1;

EOF

  for country in US GB DE CA JP BR CN FR IT IN; do
    for vertical in Travel Finance Shopping Autos Business; do
      crome_ga_hosts "${country}" "${rprefix}" "${1:-$( days_from_now 0 )}" "${vertical}"
    done
  done
}

market_share_ga "${@}"
