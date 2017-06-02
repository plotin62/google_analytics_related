#!/bin/bash
#
# go/infrastoretables
# measure.milligcu_limit
# https://cs.corp.google.com/#piper///depot/google3/monitoring/utilization/borg/proto/common.proto&sq=package:piper%20file://depot/google3%20-file:google3/(experimental%7Cobsolete)&type=cs&l=20&rcl=97733307
# enum WidePriorityBand {
#   TESTING             = 0;
#   BATCH               = 1;
#   NONPRODUCTION       = 2;
#   PRODUCTION          = 3;
#   DEDICATED           = 4;
#   MONITORING          = 5;
#
# Cells to look up here:
# https://cs.corp.google.com/piper///depot/google3/production/borg/analytics/backend/env/production/environment.gcl?q=file:analytics/backend/env/production/environment%20processing%5C%20=&l=36


source "$( dirname $0 )/upload_metric.sh"
source "$( dirname $0 )/util.sh"

set -x

function milligcu_by_jobs() {

  local date="${1:-$( days_from_now 0 )}"
  local path="/cns/ig-d/home/aredakov/resources_usage_averaged/${date}"
  local svg="$( tempfile --prefix modelhits --suffix .svg )"
  local svg_b="$( tempfile --prefix modelhits --suffix .svg )"
  local svg_all="$( tempfile --prefix modelhits --suffix .svg )"

  tee /dev/stderr << EOF | dremel
  ${DREMEL_INIT}

  # borgcfg /google/src/files/head/depot/google3/production/borg/analytics/backend/env/production/environment.gcl print environment.shards.shard1.borg_cells_lists.processing
  MATERIALIZE '${path}/gcu_by_jobs_daily/data@1' AS
  SELECT date,
    category,
    SUM(collection_gcu_limit) AS gcu_limit
  FROM
    (SELECT
      STRFTIME_USEC(timestamp, "%Y/%m/%d") AS date,
       key.cell,
       key.collection_uid,
        CASE
          WHEN REGEXP(trim(key.logical_name), r'(lexie|reprocessing)') THEN 'reprocessing'
          WHEN REGEXP(trim(key.logical_name), r'hits') THEN 'logs processing'
          WHEN REGEXP(key.logical_name, r'(premium-extractor|offline_ext)') THEN 'premium extractor'
          WHEN REGEXP(key.logical_name, r'(precompute|premium|preload|columniz)') THEN 'premium processing'
          WHEN REGEXP(key.logical_name, r'(session|aggregation|bigfunnels|snapshots|audience|config|sorting|kansas)') THEN 'processing'
          WHEN REGEXP(key.logical_name, r'(cheetah|tiger|anteater|camel)') THEN 'realtime'
          WHEN REGEXP(key.logical_name, r'(remarketing|badger)') THEN 'remarketing'
          WHEN REGEXP(key.logical_name, r'(extractor)') THEN 'extractor'
        ELSE 'other'
        END AS category,
       ROUND(AVG(measure.milligcu_limit)/1000,2) AS collection_gcu_limit,
    FROM rumbo.borglet_job_measures_by_priorityband.last100days
    WHERE key.cell IN ('de','ih','iq','ld','le','lf','pb','pc','qd','sa','sb',
      'sc','tg','th','vk','wi','wj','wl','yl','yv','iy', 'on', 'pb', 'qk')
    AND key.user IN ("urchin-processing", "urchin-logs-processing",
      "logreader-urchin", "analytics-processing-dev", "analytics-realtime")
    GROUP@50 BY 1,2,3,4
    HAVING DATEDIFF(NOW(), PARSE_TIME_USEC(date)) > 3)
  GROUP@50 BY 1,2;

  MATERIALIZE '${path}/gcu_processing/data@1' AS
  SELECT date,
    category,
    SUM(collection_gcu_limit) AS gcu_limit
  FROM
    (SELECT
      STRFTIME_USEC(timestamp, "%Y/%m/%d") AS date,
       key.cell,
       key.collection_uid,
       EXTRACT_REGEXP(key.logical_name, r'(session|aggregation|bigfunnels|snapshots|audience|config|sorting|kansas)') AS category,
       ROUND(AVG(measure.milligcu_limit)/1000,2) AS collection_gcu_limit,
    FROM rumbo.borglet_job_measures_by_priorityband.last100days
    WHERE key.cell IN ('de','ih','iq','ld','le','lf','pb','pc','qd','sa','sb',
      'sc','tg','th','vk','wi','wj','wl','yl','yv','iy', 'on', 'pb', 'qk')
      AND key.user IN ("urchin-processing", "urchin-logs-processing",
      "logreader-urchin", "analytics-processing-dev", "analytics-realtime")
      AND EXTRACT_REGEXP(key.logical_name, r'(session|aggregation|bigfunnels|snapshots|audience|config|sorting|kansas)') != ''
    GROUP@50 BY 1,2,3,4
    HAVING DATEDIFF(NOW(), PARSE_TIME_USEC(date)) > 3)
  GROUP@50 BY 1,2;

  MATERIALIZE 'csv:${path}/batch_mean_gcu_limit/data.csv@1 header:true' AS
  SELECT category,
    ROUND(SUM(collection_gcu_limit)/7,2) AS batch_mean_gcu_limit
  FROM
    (SELECT
      STRFTIME_USEC(timestamp, "%Y/%m/%d") AS date,
       key.cell,
       key.collection_uid,
        CASE
          WHEN REGEXP(trim(key.logical_name), r'(lexie|reprocessing)') THEN 'reprocessing'
          WHEN REGEXP(trim(key.logical_name), r'hits') then 'logs_processing'
          WHEN REGEXP(key.logical_name, r'(premium-extractor|offline_ext)') THEN 'premium_extractor'
          WHEN REGEXP(key.logical_name, r'(precompute|premium|preload|columniz)') THEN 'premium_processing'
          WHEN REGEXP(key.logical_name, r'(session|aggregation|bigfunnels|snapshots|audience|config|sorting|kansas)') THEN 'processing'
          WHEN REGEXP(key.logical_name, r'(cheetah|tiger|anteater|camel)') THEN 'realtime'
          WHEN REGEXP(key.logical_name, r'(remarketing|badger)') THEN 'remarketing'
          WHEN REGEXP(key.logical_name, r'(extractor)') THEN 'extractor'
        ELSE 'other'
        END AS category,
       ROUND(AVG(measure.milligcu_limit)/1000,2) AS collection_gcu_limit,
    FROM rumbo.borglet_job_measures_by_priorityband.last10days
    WHERE key.cell IN ('de','ih','iq','ld','le','lf','pb','pc','qd','sa','sb',
      'sc','tg','th','vk','wi','wj','wl','yl','yv','iy', 'on', 'pb', 'qk')
    AND key.user IN ("urchin-processing", "urchin-logs-processing",
      "logreader-urchin", "analytics-processing-dev", "analytics-realtime")
    AND key.accounting_user = 'batch_scheduler'
    GROUP@50 BY 1,2,3,4
    HAVING DATEDIFF(NOW(), PARSE_TIME_USEC(date)) > 2)
  GROUP@50 BY 1
  ORDER BY 2;

  MATERIALIZE 'csv:${path}/nonbatch_mean_gcu_limit/data.csv@1 header:true' AS
  SELECT category,
    ROUND(SUM(collection_gcu_limit)/7,2) AS nonbatch_mean_gcu_limit
  FROM
    (SELECT
      STRFTIME_USEC(timestamp, "%Y/%m/%d") AS date,
       key.cell,
       key.collection_uid,
        CASE
          WHEN REGEXP(trim(key.logical_name), r'(lexie|reprocessing)') THEN 'reprocessing'
          WHEN REGEXP(trim(key.logical_name), r'hits') then 'logs_processing'
          WHEN REGEXP(key.logical_name, r'(premium-extractor|offline_ext)') THEN 'premium_extractor'
          WHEN REGEXP(key.logical_name, r'(precompute|premium|preload|columniz)') THEN 'premium_processing'
          WHEN REGEXP(key.logical_name, r'(session|aggregation|bigfunnels|snapshots|audience|config|sorting|kansas)') THEN 'processing'
          WHEN REGEXP(key.logical_name, r'(cheetah|tiger|anteater|camel)') THEN 'realtime'
          WHEN REGEXP(key.logical_name, r'(remarketing|badger)') THEN 'remarketing'
          WHEN REGEXP(key.logical_name, r'(extractor)') THEN 'extractor'
        ELSE 'other'
        END AS category,
       ROUND(AVG(measure.milligcu_limit)/1000,2) AS collection_gcu_limit,
    FROM rumbo.borglet_job_measures_by_priorityband.last10days
    WHERE key.cell IN ('de','ih','iq','ld','le','lf','pb','pc','qd','sa','sb',
      'sc','tg','th','vk','wi','wj','wl','yl','yv','iy', 'on', 'pb', 'qk')
    AND key.user IN ("urchin-processing", "urchin-logs-processing",
      "logreader-urchin", "analytics-processing-dev", "analytics-realtime")
    AND key.accounting_user != 'batch_scheduler'
    GROUP@50 BY 1,2,3,4
    HAVING DATEDIFF(NOW(), PARSE_TIME_USEC(date)) > 2)
  GROUP@50 BY 1
  ORDER BY 2;

EOF

  tee /dev/stderr << EOF | R --vanilla
  library(ginstall)
  library(gfile)
  library(namespacefs)
  library(rglib)
  library(cfs)
  library(dremel)
  library(ggplot2)
  library(scales)
  library(directlabels)
  InitGoogle()
  options("scipen"=100, "digits"=6)

  myConn <- DremelConnect()
  DremelAddTableDef('gcu_jobs_daily', '${path}/gcu_by_jobs_daily/data*', myConn, verbose=FALSE)

  d <- DremelExecuteQuery("SELECT date, category, gcu_limit
    FROM gcu_jobs_daily
    WHERE category != 'reprocessing'
    GROUP BY 1,2,3;", myConn)
  d\$date <- as.Date(d\$date)
  d\$category <- factor(d\$category, levels = c('processing', 'extractor','premium processing',
    'premium extractor','logs processing', 'realtime','remarketing','other'))

  # locally weighted regression
  svg("${svg}")
    pd <- position_dodge(.1)
    ggplot(d, aes(date, gcu_limit/1000)) +
    facet_wrap(~category, ncol=2, scales="free") +
    theme(strip.text=element_text(size=8)) +
    geom_line(position=pd,aes(group=category),color="chartreuse4") +
    stat_smooth(method="loess", fullrange=TRUE, size=1.2, span=.2) +
    theme(axis.text.x=element_text(size=8,face="bold", color="gray26")) +
    theme(axis.text.y=element_text(size=8,face="bold", color="gray26")) +
    ylab("GCU Limit, K") + xlab("")
  dev.off()

  # Overall view.
  svg("${svg_all}")
    rhg_cols <- c("#771C19","#AA3929","#E25033","#F27314","#F8A31B","#E2C59F",
      "#B6C5CC","#8E9CA3","#556670","#000000")
    pd <- position_dodge(.1)
    ggplot(d, aes(x = date, y = gcu_limit/1000, fill = category)) +
    geom_area(stat="smooth",method="loess", span=.2) +
    ylab("GCU Limit, K") + xlab("") +
    scale_fill_manual(values = rhg_cols) +
    theme(axis.text.x = element_text(face="bold", color="gray26",size=8)) +
    theme(axis.text.y = element_text(face="bold", color="gray26",size=8)) +
    guides(fill = guide_legend(reverse=TRUE)) +
    theme(legend.title=element_blank())
  dev.off()

  DremelAddTableDef('gcu_processing', '${path}/gcu_processing/data*', myConn, verbose=FALSE)
  g <- DremelExecuteQuery("SELECT date, category, gcu_limit FROM gcu_processing GROUP BY 1,2,3;", myConn)
  g\$date <- as.Date(g\$date)

  svg("${svg_b}")
    pd <- position_dodge(.1)
    ggplot(g, aes(date, gcu_limit/1000)) +
    facet_wrap(~category, ncol=2, scales="free") +
    theme(strip.text=element_text(size=8)) +
    geom_line(position=pd,aes(group=category),color="chartreuse4") +
    stat_smooth(method="loess", fullrange=TRUE, size=1.2) +
    theme(axis.text.x=element_text(size=5, vjust=0.5)) +
    theme(axis.ticks.x=element_blank()) +
    theme(axis.text.y=element_text(size=5, vjust=0.5)) +
    theme(axis.ticks.y=element_blank()) +
    ylab("Processing GCU Limit, K") + xlab("")
  dev.off()

EOF

  fileutil cp -f "${path}/batch_mean_gcu_limit/data.csv-00000-of-00001" /tmp/batch_mean_gcu_limit.csv
  fileutil cp -f "${path}/nonbatch_mean_gcu_limit/data.csv-00000-of-00001" /tmp/nonbatch_mean_gcu_limit.csv 

  echo ''| sendgmr --subject="GCU Limit, computed on ${date}" --attachment_files="${svg}","${svg_all}","${svg_b}",/tmp/batch_mean_gcu_limit.csv,/tmp/nonbatch_mean_gcu_limit.csv  \
  --to=aredakov@google.com,chisan@google.com,kavan@google.com

}

milligcu_by_jobs
