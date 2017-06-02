#!/bin/bash
#
# Extracting predicted conversion rates per channel from DDA BE.
# Verification in gqui
# Path stats
# from /bigtable/srv-iy-urchin/urchin-processing.production.bigfunnels.data_driven_prediction:prediction_models select  \
# path_stats[0].path_key.name, path_stats[0].expected_number_of_conversions, \
# path_stats[0].path_count, \
# path_stats[1].path_key.name, path_stats[1].expected_number_of_conversions, \
# path_stats[1].path_count \
# where key_ = '1:934:81299439:20161204' \
# and meta_.column = 'prediction_models:TRANSACTION'
#
# Event names
# from /bigtable/srv-iy-urchin/urchin-processing.production.bigfunnels.data_driven_prediction:prediction_models select  \
# key_ , model_context.channel_grouping_type, \
# model_context.event_names.entry[0].event_id, \
# model_context.event_names.entry[0].event_name, \
# model_context.event_names.entry[1].event_id, \
# model_context.event_names.entry[1].event_name, \
# model_context.event_names.entry[2].event_id, \
# model_context.event_names.entry[2].event_name \
# where key_ = '1:934:81299439:20161204' \
# and meta_.column = 'prediction_models:TRANSACTION'

source gbash.sh || exit 1

function main () {

  tee /dev/stderr << EOF | dremel
  SET accounting_group analytics-internal-processing-dev;
  SET min_completion_ratio 1;
  SET io_timeout 2400;
  SET runtime_name dremel;
  SET materialize_overwrite true;
  SET materialize_owner_group analytics-internal-processing-dev;

  DEFINE TABLE ddao <<EOF
    bigtable2:
      bigtable_name: "/bigtable/srv-iy-urchin/urchin-processing.production.bigfunnels.data_driven_prediction"
    default_timestamp_mode: LATEST
  EOF;

  DEFINE TABLE ddaa <<EOF
    bigtable2:
      bigtable_name: "/bigtable/srv-iy-urchin/urchin-processing.shard1.bigfunnels.data_driven_prediction"
    default_timestamp_mode: 0
  EOF;

  MATERIALIZE '/cns/ig-d/home/aredakov/dda_conversions_rates_multy/event_id_name_zero/data@500' AS
    SELECT
    name_rowkey,
    event_name,
    event_id
  FROM
    (SELECT
      rowkey AS name_rowkey,
      prediction_models.column.cell.Value.model_context.event_names.entry.event_name
        AS event_name,
      STRING(prediction_models.column.cell.Value.model_context.event_names.entry.event_id)
        AS event_id,
      FIRST_VALUE(prediction_models.column.cell.timestamp) OVER
        (PARTITION BY rowkey ORDER BY prediction_models.column.cell.timestamp DESC)
        AS timestamp
    FROM FLATTEN(FLATTEN(ddao, prediction_models.column.cell),
      prediction_models.column.cell.Value.model_context.event_names.entry)
    WHERE prediction_models.column.cell.Value.model_context.channel_grouping_type
      = 'GA')
  GROUP@500 BY 1,2,3;

  MATERIALIZE '/cns/ig-d/home/aredakov/dda_conversions_rates_multy/event_id_name_one/data@500' AS
  SELECT
    name_rowkey,
    event_name,
    event_id
  FROM
    (SELECT
      rowkey AS name_rowkey,
      prediction_models.column.cell.Value.model_context.event_names.entry.event_name
        AS event_name,
      STRING(prediction_models.column.cell.Value.model_context.event_names.entry.event_id)
        AS event_id,
      FIRST_VALUE(prediction_models.column.cell.timestamp) OVER
        (PARTITION BY rowkey ORDER BY prediction_models.column.cell.timestamp DESC)
        AS timestamp
    FROM FLATTEN(FLATTEN(ddaa, prediction_models.column.cell),
      prediction_models.column.cell.Value.model_context.event_names.entry)
    WHERE prediction_models.column.cell.Value.model_context.channel_grouping_type
      = 'GA')
  GROUP@500 BY 1,2,3;

  MATERIALIZE '/cns/ig-d/home/aredakov/dda_conversions_rates_multy/transaction_probabilities_zero/data@500' AS
  SELECT
    rowkey,
    id_name,
    a_event,
    b_event,
    c_event,
    ANY(expected_conversions) AS expected_conversions
  FROM
    (SELECT
      rowkey,
      prediction_models.column.cell.Value.path_stats.path_key.name AS id_name,
      EXTRACT_REGEXP(prediction_models.column.cell.Value.path_stats.path_key.name,
       '([0-9]*)*.*') AS a_event,
      EXTRACT_REGEXP(prediction_models.column.cell.Value.path_stats.path_key.name,
       '[^:]*:([0-9]*).*') AS b_event,
      EXTRACT_REGEXP(prediction_models.column.cell.Value.path_stats.path_key.name,
      '[^:]*:[^:]*:([0-9]*).*') AS c_event,
      prediction_models.column.cell.Value.path_stats.expected_number_of_conversions
        AS expected_conversions,
     FIRST_VALUE(prediction_models.column.cell.timestamp) OVER
      (PARTITION BY rowkey ORDER BY prediction_models.column.cell.timestamp DESC)
      AS timestamp
    FROM FLATTEN(FLATTEN(ddao, prediction_models.column.cell),
       prediction_models.column.cell.Value.path_stats)
    WHERE prediction_models.column.name = 'TRANSACTION'
      AND EXTRACT_REGEXP(prediction_models.column.cell.Value.path_stats.path_key.name,
      '[^:]*:[^:]*:([0-9]*).*') != ''
      AND rowkey CONTAINS ":2016")
  GROUP@500 BY 1,2,3,4,5;

  MATERIALIZE '/cns/ig-d/home/aredakov/dda_conversions_rates_multy/transaction_probabilities_one/data@500' AS
   SELECT
    rowkey,
    id_name,
    a_event,
    b_event,
    c_event,
    ANY(expected_conversions) AS expected_conversions
  FROM
    (SELECT
      rowkey,
      prediction_models.column.cell.Value.path_stats.path_key.name AS id_name,
      EXTRACT_REGEXP(prediction_models.column.cell.Value.path_stats.path_key.name,
       '([0-9]*)*.*') AS a_event,
      EXTRACT_REGEXP(prediction_models.column.cell.Value.path_stats.path_key.name,
       '[^:]*:([0-9]*).*') AS b_event,
      EXTRACT_REGEXP(prediction_models.column.cell.Value.path_stats.path_key.name,
      '[^:]*:[^:]*:([0-9]*).*') AS c_event,
      prediction_models.column.cell.Value.path_stats.expected_number_of_conversions
        AS expected_conversions,
     FIRST_VALUE(prediction_models.column.cell.timestamp) OVER
      (PARTITION BY rowkey ORDER BY prediction_models.column.cell.timestamp DESC)
      AS timestamp
    FROM FLATTEN(FLATTEN(ddaa, prediction_models.column.cell),
       prediction_models.column.cell.Value.path_stats)
    WHERE prediction_models.column.name = 'TRANSACTION'
      AND EXTRACT_REGEXP(prediction_models.column.cell.Value.path_stats.path_key.name,
      '[^:]*:[^:]*:([0-9]*).*') != ''
      AND rowkey CONTAINS ":2016")
  GROUP@500 BY 1,2,3,4,5;

  DEFINE TABLE event_id_name_one /cns/ig-d/home/aredakov/dda_conversions_rates_multy/event_id_name_one/data*;
  DEFINE TABLE event_id_name_zero /cns/ig-d/home/aredakov/dda_conversions_rates_multy/event_id_name_zero/data*;
  MATERIALIZE '/cns/ig-d/home/aredakov/dda_conversions_rates_multy/event_names_joined/data@50' AS
  SELECT
    name_rowkey,
    event_name,
    event_id
  FROM event_id_name_one, event_id_name_zero;

  DEFINE TABLE transaction_probabilities_one /cns/ig-d/home/aredakov/dda_conversions_rates_multy/transaction_probabilities_one/data*;
  DEFINE TABLE transaction_probabilities_zero /cns/ig-d/home/aredakov/dda_conversions_rates_multy/transaction_probabilities_zero/data*;
  MATERIALIZE '/cns/ig-d/home/aredakov/dda_conversions_rates_multy/expected_conversions_joined/data@500' AS
  SELECT
    rowkey,
    id_name,
    a_event,
    b_event,
    c_event,
    expected_conversions,
    path_count
  FROM transaction_probabilities_one, transaction_probabilities_zero;

  DEFINE TABLE event_names_joined /cns/ig-d/home/aredakov/dda_conversions_rates_multy/event_names_joined/data*;
  DEFINE TABLE expected_conversions_joined /cns/ig-d/home/aredakov/dda_conversions_rates_multy/expected_conversions_joined/data*;
  MATERIALIZE '/cns/ig-d/home/aredakov/dda_conversions_rates_multy/dda_final_data/data@500' AS
  SELECT
    rowkey,
    full_path,
    a_event_name,
    b_event_name,
    c_event_name,
    LAST(expected_conversions) AS expected_conversions,
    LAST(path_count) AS path_count
  FROM
    (SELECT
      tt.rowkey,
      tt.id_name AS full_path,
      t1.event_name AS a_event_name,
      t2.event_name AS b_event_name,
      t3.event_name AS c_event_name,
      LAST(tt.expected_conversions) AS expected_conversions,
      LAST(tt.path_count) AS path_count
    FROM expected_conversions_joined tt
    JOIN@500 event_names_joined AS t1
    ON tt.rowkey = t1.name_rowkey AND tt.a_event = t1.event_id
    JOIN@500 event_names_joined AS t2
    ON tt.rowkey = t2.name_rowkey AND tt.b_event = t2.event_id
    JOIN@500 event_names_joined AS t3
    ON tt.rowkey = t3.name_rowkey AND tt.c_event = t3.event_id
    GROUP@500 BY 1,2,3,4,5)
  GROUP@500 BY 1,2,3,4,5;

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
  library(lubridate)
  library(Hmisc)
  library(nlme)
  library(lme4)
  library(data.table)
  library(RColorBrewer)
  InitGoogle()
  options("scipen"=100, "digits"=4)

  myConn <- DremelConnect()
  DremelSetMinCompletionRatio(myConn, 1.0)
  DremelSetAccountingGroup(myConn,'urchin-processing-qa')
  DremelSetMaterializeOwnerGroup(myConn, 'materialize-a-dremel')
  DremelSetMaterializeOverwrite(myConn, TRUE)
  DremelSetIOTimeout(myConn, 7200)

  DremelAddTableDef('dda_final_data', '/cns/ig-d/home/aredakov/dda_conversions_rates_multy/dda_final_data/data*',
      myConn, verbose=FALSE)

  # Display assisted:TR.
  as_dis <- DremelExecuteQuery("
    SELECT
      date,
      view_id,
      c_event_name AS last_event,
      ROUND(expected_conversions,6) AS expected_conversions
    FROM dda_final_data
    WHERE (a_event_name = 'Display' OR b_event_name = 'Display')
      AND date CONTAINS '2016'
  ;", myConn)

  as_dis$last_event <- factor(as_dis$last_event, levels = c('Direct',
    'Organic Search','Paid Search','Generic Paid Search',
    'Branded Paid Search','(Other)','Email','Referral','Social','Display'))
  medians <- ddply(as_dis, .(last_event), summarise,
    med = median(expected_conversions))

  ggplot(as_dis, aes(x=last_event, y=expected_conversions, fill=last_event)) +
  geom_boxplot(outlier.shape = NA) +
  geom_text(data = medians, aes(x = last_event, y = med,
    label=sprintf("%1.2f%%", med*100)), size = 3, vjust = -2) +
  coord_cartesian(ylim = c(0, 0.25)) +
  theme_bw() +
  scale_y_continuous(labels = percent) +
  theme(legend.position="none") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  ggtitle("Display Assisted. Transactional Rates per Date / Ecommerce Site.") +
  theme(plot.title = element_text(size = 12, face = "bold")) +
  ylab("Transactional Rates") + xlab("")

  # Display assisted:Transactional volume.
  tv_dis <- DremelExecuteQuery("
    SELECT
      view_id,
      c_event_name AS last_event,
      SUM(path_count * expected_conversions) AS tr_volume
    FROM dda_final_data
    WHERE (a_event_name = 'Display' OR b_event_name = 'Display')
      AND date CONTAINS '2016'
    GROUP@50 BY 1,2
  ;", myConn)

  tv_dis$last_event <- factor(tv_dis$last_event, levels = c('Direct',
    'Organic Search','Paid Search','Generic Paid Search',
    'Branded Paid Search','(Other)','Email','Referral','Social','Display'))
  medians <- ddply(tv_dis, .(last_event), summarise,
    med = median(tr_volume))

  ggplot(tv_dis, aes(x=last_event, y=tr_volume, fill=last_event)) +
  geom_boxplot(outlier.shape = NA) +
  geom_text(data = medians, aes(x = last_event, y = med,
    label=sprintf("%1.0f", med)), size = 3, vjust = -3) +
  coord_cartesian(ylim = c(0, 10000)) +
  theme_bw() +
  theme(legend.position="none") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  ggtitle("Display Assisted. Transactional Volume per Ecommerce Site.") +
  theme(plot.title = element_text(size = 12, face = "bold")) +
  ylab("Transactional Volume") + xlab("")

  # Social assisted:TR.
  as_soc <- DremelExecuteQuery("
    SELECT
      date,
      view_id,
      c_event_name AS last_event,
      ROUND(expected_conversions,6) AS expected_conversions
    FROM dda_final_data
    WHERE (a_event_name = 'Social' OR b_event_name = 'Social')
      AND date CONTAINS '2016'
  ;", myConn)

  as_soc$last_event <- factor(as_soc$last_event, levels = c('Direct',
    'Organic Search','Paid Search','Generic Paid Search',
    'Branded Paid Search','(Other)','Email','Referral','Social','Display'))

  medians <- ddply(as_soc, .(last_event), summarise,
    med = median(expected_conversions))
  dodge <- position_dodge(width = 0.4)
  ggplot(as_soc, aes(x=last_event, y=expected_conversions, fill=last_event)) +
  geom_boxplot(outlier.shape = NA) +
  geom_text(data = medians, aes(x = last_event, y = med,
    label=sprintf("%1.2f%%", med*100)), size = 3, vjust = -2) +
  coord_cartesian(ylim = c(0, 0.25)) +
  theme_bw() +
  scale_y_continuous(labels = percent) +
  theme(legend.position="none") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  ggtitle("Social Assisted. Transactional Rates per Date / Ecommerce Site.") +
  theme(plot.title = element_text(size = 12, face = "bold")) +
  ylab("Transactional Rates") + xlab("")

  # Social assisted:Transactional volume.
  tv_soc <- DremelExecuteQuery("
    SELECT
      view_id,
      c_event_name AS last_event,
      SUM(path_count * expected_conversions) AS tr_volume
    FROM dda_final_data
    WHERE (a_event_name = 'Social' OR b_event_name = 'Social')
      AND date CONTAINS '2016'
    GROUP@50 BY 1,2
  ;", myConn)

  tv_soc$last_event <- factor(tv_soc$last_event, levels = c('Direct',
    'Organic Search','Paid Search','Generic Paid Search',
    'Branded Paid Search','(Other)','Email','Referral','Social','Display'))
  medians <- ddply(tv_soc, .(last_event), summarise, med = median(tr_volume))

  ggplot(tv_soc, aes(x=last_event, y=tr_volume, fill=last_event)) +
  geom_boxplot(outlier.shape = NA) +
  geom_text(data = medians, aes(x = last_event, y = med,
    label=sprintf("%1.0f", med)), size = 3, vjust = -3) +
  coord_cartesian(ylim = c(0, 10000)) +
  theme_bw() +
  theme(legend.position="none") +
  theme(legend.position="none") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  ggtitle("Social Assisted. Transactional Volume per Ecommerce Site.") +
  theme(plot.title = element_text(size = 12, face = "bold")) +
  ylab("Transactional Volume") + xlab("")

  # t-tests.
  dis_d <- as_dis[ which(as_dis$last_event == 'Direct'), ]
  dis_s <- as_dis[ which(as_dis$last_event == 'Organic Search'), ]

  soc_d <- as_soc[ which(as_soc$last_event == 'Direct'), ]
  soc_s <- as_soc[ which(as_soc$last_event == 'Organic Search'), ]

  describe(dis_d$expected_conversions)
  describe(dis_s$expected_conversions)

  describe(soc_d$expected_conversions)
  describe(soc_s$expected_conversions)

  t.test(dis_d$expected_conversions,soc_d$expected_conversions)

  t.test(dis_s$expected_conversions,soc_s$expected_conversions)

  plot(density(dis_d$expected_conversions),
    col = "blue",
    main = "Display(blue) vs Social (red) Assisting Direct",
    xlab = "Transactional rates")
  lines(density(soc_d$expected_conversions), col = "red")

  plot(density(dis_s$expected_conversions),
    col = "blue",
    main = "Display(blue) vs Social (red) Assisting Organic Search",
    xlab = "Transactional rates")
  lines(density(soc_s$expected_conversions), col = "red")

EOF

}

gbash::main "$@"
