#!/bin/bash

library(ginstall)
library(gfile)
library(namespacefs)
library(rglib)
library(cfs)
library(dremel)
library(gbm)
library(Hmisc)
library(ggplot2)
library(scales)
library(directlabels)
library(lubridate)
InitGoogle()
options("scipen"=100, "digits"=12)

myConn <- DremelConnect()
DremelSetMinCompletionRatio(myConn, 1.0)
DremelSetAccountingGroup(myConn,'urchin-processing-qa')
DremelSetMaterializeOwnerGroup(myConn, 'materialize-a-dremel')
DremelSetMaterializeOverwrite(myConn, TRUE)
DremelSetIOTimeout(myConn, 7200)

g <- DremelExecuteQuery("
  SELECT 
    property_id,
    IF(vertical_name IS NULL,'Unknown',vertical_name) AS vertical_name,
    (transactions / visits) AS gindex
  FROM analytics_datamart_basic.properties_transactions.latest
  WHERE source = 'google'
    AND(vertical_name IN ('Shopping', 'Travel','Home_Garden','Beauty_Fitness',
      'Food_Drink','Autos_Vehicles','Finance','Arts_Entertainment')
      OR vertical_name IS NULL)
    AND qos != 'premium'
  HAVING gindex <= 1
;", myConn)
# g <- na.omit(g)

f <- DremelExecuteQuery("
  SELECT 
    property_id,
    IF(vertical_name IS NULL,'Unknown',vertical_name) AS vertical_name,
    (transactions / visits) AS findex
  FROM analytics_datamart_basic.properties_transactions.latest
  WHERE source = 'facebook'
  AND(vertical_name IN ('Shopping', 'Travel','Home_Garden','Beauty_Fitness',
      'Food_Drink','Autos_Vehicles','Finance','Arts_Entertainment')
      OR vertical_name IS NULL)
  AND qos != 'premium'
  HAVING findex <= 1
;", myConn)
# f <- na.omit(f)

aggregate(g[, 3], list(Name=g$vertical_name), mean)
aggregate(f[, 3], list(Name=f$vertical_name), mean)

# And Google and Facebook present on the same property
gf <- DremelExecuteQuery("
  SELECT 
    a.date AS date, 
    a.vertical_name AS vertical_name, 
    a.property_id AS property_id,
    gindex, 
    findex,
    gbounces,
    fbounces,
  FROM 
  (SELECT 
      date,
      property_id,
      vertical_name,
      (bounces / visits) AS gbounces,
      (transactions / visits) AS gindex
    FROM analytics_datamart_basic.properties_transactions.latest
    WHERE source = 'google'
      AND vertical_name IN ('Shopping', 'Travel','Home_Garden','Beauty_Fitness',
        'Food_Drink','Autos_Vehicles','Finance','Arts_Entertainment')
      AND qos = 'premium'
      AND country_code ='US'
    HAVING gindex <= 1) a
  JOIN@50
  (SELECT 
      date,
      property_id,
      vertical_name,
      (bounces / visits) AS fbounces,
      (transactions / visits) AS findex
    FROM analytics_datamart_basic.properties_transactions.latest
    WHERE source = 'facebook'
      AND vertical_name IN ('Shopping', 'Travel','Home_Garden','Beauty_Fitness',
        'Food_Drink','Autos_Vehicles','Finance','Arts_Entertainment')
    AND qos = 'premium'
    AND country_code ='US'
    HAVING findex <= 1) b
  ON a.date = b.date AND a.property_id = b.property_id  AND  a.vertical_name = b.vertical_name
;", myConn)
gf <- na.omit(gf)
aggregate(gf[, 4:7], list(Name=gf$vertical_name), mean)


# All verticals
g <- DremelExecuteQuery("
  SELECT
    property_id,
    (bounces / visits) AS gbounces,
    (transactions / visits) AS gindex
  FROM analytics_datamart_basic.properties_transactions.latest
  WHERE source = 'google'
    AND qos != 'premium'
    AND country_code = 'US'
    AND vertical_name IS NULL
  HAVING gindex <= 1
;", myConn)
g <- na.omit(g)

f <- DremelExecuteQuery("
  SELECT
    property_id,
    (bounces / visits) AS fbounces,
    (transactions / visits) AS findex
  FROM analytics_datamart_basic.properties_transactions.latest
  WHERE source = 'facebook'
    AND qos != 'premium'
    AND country_code = 'US'
    AND vertical_name IS NULL
  HAVING findex <= 1
;", myConn)
f <- na.omit(f)

t.test(g$gindex, f$findex)
t.test(g$gbounces, f$fbounces)

# And Google and Facebook present on the same property
gf <- DremelExecuteQuery("
  SELECT 
    gindex, 
    findex,
    gbounces,
    fbounces,
  FROM 
  (SELECT 
      property_id,
      (bounces / visits) AS gbounces,
      (transactions / visits) AS gindex
    FROM analytics_datamart_basic.properties_transactions.latest
    WHERE source = 'google'
      AND qos = 'premium'
      AND country_code = 'US'
    HAVING gindex <= 1) a
  JOIN@50
  (SELECT 
      property_id,
      (bounces / visits) AS fbounces,
      (transactions / visits) AS findex
    FROM analytics_datamart_basic.properties_transactions.latest
    WHERE source = 'facebook'
      AND qos = 'premium'
      AND country_code = 'US'
    HAVING findex <= 1) b
  ON a.property_id = b.property_id;
;", myConn)
gf <- na.omit(gf)

t.test(gf$gindex, gf$findex)
t.test(gf$gbounces, gf$fbounces)


# Regressions
library(ginstall)
library(gfile)
library(namespacefs)
library(rglib)
library(cfs)
library(dremel)
library(gbm)
library(Hmisc)
library(ggplot2)
library(scales)
library(directlabels)
library(lubridate)
InitGoogle()
options("scipen"=100, "digits"=12)

myConn <- DremelConnect()
DremelSetMinCompletionRatio(myConn, 1.0)
DremelSetAccountingGroup(myConn,'urchin-processing-qa')
DremelSetMaterializeOwnerGroup(myConn, 'materialize-a-dremel')
DremelSetMaterializeOverwrite(myConn, TRUE)
DremelSetIOTimeout(myConn, 7200)

g <- DremelExecuteQuery("
  SELECT 
    date,
    DATEDIFF(STRING(date), '20160627') AS days,
    AVG(transactions / visits) AS mean_gindex,
    SUM(transactions) AS sum_transactions
  FROM analytics_datamart_basic.properties_transactions.latest
  WHERE source = 'google'
    AND qos = 'premium'
  GROUP BY 1,2
  HAVING mean_gindex <= 1
;", myConn)
g <- na.omit(g)
fit <- lm(mean_gindex ~ days, data=g)
fitt <- lm(sum_transactions ~ days, data=g)

summary(fit)
days        0.00002734203247 p-value << 0.05

summary(fitt)
days          13297    p-value << 0.05

plot(g$days, g$mean_gindex,
  main = "Google Conversion Rate Dynamic",
  xlab = "",
  ylab = "Conversion Rate",
  col = "darkred",
  pch=16,
  cex=0.8,
  cex.axis=0.6)
grid(lty = 6, col = "cornsilk2")
axis(1, at = g$days,
  labels = paste("\n", g$date ),
  padj = 1,
  col.axis="darkred",
  cex.axis=0.8,
  tck=0)
lines(g$days, predict(fit),col = "darkblue",lwd=1.5)
r2 <- round(summary(fit)$r.squared, 2)
modsum <- summary(fit)
my.p1 <- round(modsum$coefficients[2,4],3)
pval <- bquote(
  italic(p) == .(format(my.p1))*","
  ~~ r^2 == .(r2))
mtext(pval, side =3, col ="darkblue", cex=0.6, font=2)

plot(g$days, g$sum_transactions,
  main = "Google Transaction Dynamic",
  xlab = "",
  ylab = "Transactions",
  col = "darkred",
  pch=16,
  cex=0.8,
  cex.axis=0.6)
grid(lty = 6, col = "cornsilk2")
axis(1, at = g$days,
  labels = paste("\n", g$date ),
  padj = 1,
  col.axis="darkred",
  cex.axis=0.8,
  tck=0)
lines(g$days, predict(fitt),col = "darkblue",lwd=1.5)
r2 <- round(summary(fitt)$r.squared, 2)
modsum <- summary(fitt)
my.p1 <- round(modsum$coefficients[2,4],3)
pval <- bquote(
  italic(p) == .(format(my.p1))*","
  ~~ r^2 == .(r2))
mtext(pval, side =3, col ="darkblue", cex=0.6, font=2)

########################
# Facebook

f <- DremelExecuteQuery("
  SELECT 
    date,
    DATEDIFF(STRING(date), '20160627') AS days,
    AVG(transactions / visits) AS mean_findex,
    SUM(transactions) AS sum_transactions
  FROM analytics_datamart_basic.properties_transactions.latest
  WHERE source = 'facebook'
    AND qos = 'premium'
  GROUP BY 1,2
  HAVING mean_findex <= 1
;", myConn)
f <- na.omit(f)

fitf <- lm(mean_findex ~ days, data=f)
fittf <- lm(sum_transactions ~ days, data=f)

summary(fitf)
days    0.00001381988843  p-value << 0.05

summary(fittf)
days     378.5       p-value << 0.05

plot(f$days, f$mean_findex,
  main = "Facebook Conversion Rate Dynamic",
  xlab = "",
  ylab = "Conversion Rate",
  col = "darkred",
  pch=16,
  cex=0.8,
  cex.axis=0.6)
grid(lty = 6, col = "cornsilk2")
axis(1, at = f$days,
  labels = paste("\n", f$date ),
  padj = 1,
  col.axis="darkred",
  cex.axis=0.8,
  tck=0)
lines(f$days, predict(fitf),col = "darkblue",lwd=1.5)
r2 <- round(summary(fitf)$r.squared, 2)
modsum <- summary(fitf)
my.p1 <- round(modsum$coefficients[2,4],3)
pval <- bquote(
  italic(p) == .(format(my.p1))*","
  ~~ r^2 == .(r2))
mtext(pval, side =3, col ="darkblue", cex=0.6, font=2)

plot(f$days, f$sum_transactions,
  main = "Facebook Transaction Dynamic",
  xlab = "",
  ylab = "Transaction",
  col = "darkred",
  pch=16,
  cex=0.8,
  cex.axis=0.6)
grid(lty = 6, col = "cornsilk2")
axis(1, at = f$days,
  labels = paste("\n", f$date ),
  padj = 1,
  col.axis="darkred",
  cex.axis=0.8,
  tck=0)
lines(f$days, predict(fittf),col = "darkblue",lwd=1.5)
r2 <- round(summary(fittf)$r.squared, 2)
modsum <- summary(fittf)
my.p1 <- round(modsum$coefficients[2,4],3)
pval <- bquote(
  italic(p) == .(format(my.p1))*","
  ~~ r^2 == .(r2))
mtext(pval, side =3, col ="darkblue", cex=0.6, font=2)

