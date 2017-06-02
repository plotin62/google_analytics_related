#!/bin/bash

source "$( dirname $0 )/upload_metric.sh"
source "$( dirname $0 )/util.sh"

set -x

function footptint_gabe() {

  local date="${1:-$( days_from_now 0 )}"
  local pdf="$( tempfile --prefix modelhits --suffix .pdf )"

  tee /dev/stderr << EOF | R --vanilla

  library(ginstall)
  library(gfile)
  library(namespacefs)
  library(rglib)
  library(cfs)
  library(dremel)
  library(MCMCpack)
  library(lattice)
  InitGoogle()
  options("scipen"=100, "digits"=6)

  myConn <- DremelConnect()

  cells <- DremelExecuteQuery("
    SELECT date, DATEDIFF(date, '2014-08-01') AS days, cluster,
    ROUND(SUM(gcu_usage_amount)) AS gcu_usage,
    ROUND(SUM(colossus_disk_usage_amount)) AS cns_disk_usage
    FROM ads_re.resource_usage
    WHERE mdb IN ('urchin-processing', 'urchin-logs-processing',
      'logreader-urchin', 'analytics-processing-dev', 'analytics-realtime')
    AND cluster IN ('pb', 'qd','yl')
    AND PARSE_TIME_USEC(date) >= PARSE_TIME_USEC('2014-08-01')
    AND gcu_usage_amount > 0
    GROUP@50 BY 1,2,3
    HAVING gcu_usage > 10000;", myConn)

  d <- cells[ which(cells\$cluster=='pb'), ]
  d <- d[order(d\$days) , ]
  pdf("${pdf}")

  fit <- lm(gcu_usage ~ days + I(days^2), data=d)
  layout(matrix(c(1,2,3,4,5,6), nrow = 3, ncol = 2, byrow = TRUE))
  plot(d\$days, d\$gcu_usage,
    main = "PB GCU usage",
    xlab = "",
    ylab = "GCU per unit",
    col = "darkred",
    pch=16,
    cex=0.8,
    cex.axis=0.8)
  grid(lty = 6, col = "cornsilk2")
  axis(1, at = d\$days,
    labels = paste("\n", d\$date ),
    padj = 1,
    col.axis="darkred",
    cex.axis=0.7,
    tck=0)
  lines(d\$days, predict(fit),col = "darkblue",lwd=1.5)
  r2 <- round(summary(fit)\$r.squared, 2)
  modsum <- summary(fit)
  my.p1 <- round(modsum\$coefficients[2,4],3)
  my.p2 <- round(modsum\$coefficients[3,4],3)
  pval <- bquote(
    italic(p-value1) == .(format(my.p1)) *","
    ~~italic(p-value2) == .(format(my.p2)) *","
    ~~ r^2 == .(r2))
  mtext(pval, side =3, col ="darkblue", cex=0.6, font=2)

  fit_mcmc <- MCMCregress(gcu_usage ~ days, data= d, burnin = 1000, mcmc = 10000, thin = 1)
  rn <- summary(fit_mcmc)
  lower_credability <- round(rn\$quantiles[2,c(1)],1)
  bayesian_median <- round(rn\$quantiles[2,c(3)],1)
  upper_credability <- round(rn\$quantiles[2,c(5)],1)

  newd <- data.frame(days = c(max(d\$days)+30, max(d\$days)+60, max(d\$days)+90))
  pred.w.clim <- predict(fit, newd, interval = "confidence")
  latest <- (d[ which(d\$days == max(d\$days)),][1,4])
  month <- round((pred.w.clim[1,1] - latest)/latest,2)*100
  two_months <- round((pred.w.clim[2,1] - latest)/latest,2)*100
  three_months <- round((pred.w.clim[3,1] - latest)/latest,2)*100
  matplot(newd\$days, pred.w.clim,
      main = "PB Forecast",
      lty = c(1,2,2), type = "l", lwd =c(1.5,1,1),
      ylab = '',
      col = c("cadetblue4","chocolate4","chocolate4"),
      xlab = '',
      xaxt='n',
      cex=0.8,
      cex.axis=0.8)
  grid (lty = 6, col = "cornsilk2")
  legend("topleft", col = c("chocolate4"), lty = 2,
    legend = "95% Confidence interval",
    cex=0.8,
    bty = "n")
  increases <- bquote(italic(month) == .(format(month))*italic("%")*","
    ~~italic(twomonths) == .(format(two_months))*italic("%")* ","
    ~~italic(threemonths) == .(format(three_months))*italic("%"))
  mtext(increases, side =1, col ="darkblue", cex=0.6, font=2)
  bayesian <- bquote(italic(Bayes) == .(format(bayesian_median))*","
    ~~italic(low) == .(format(lower_credability))* ","
    ~~italic(up) == .(format(upper_credability)))
  mtext(bayesian, side =3, col ="darkblue", cex=0.6, font=2)

  d <- cells[ which(cells\$cluster=='qd'), ]
  d <- d[order(d\$days) , ]

  fit <- lm(gcu_usage ~ days + I(days^2), data=d)
  plot(d\$days, d\$gcu_usage,
    main = "QD GCU usage",
    xlab = "",
    ylab = "GCU per unit",
    col = "darkred",
    pch=16,
    cex=0.8,
    cex.axis=0.8)
  grid(lty = 6, col = "cornsilk2")
  axis(1, at = d\$days,
    labels = paste("\n", d\$date ),
    padj = 1,
    col.axis="darkred",
    cex.axis=0.7,
    tck=0)
  lines(d\$days, predict(fit),col = "darkblue",lwd=1.5)
  r2 <- round(summary(fit)\$r.squared, 2)
  modsum <- summary(fit)
  my.p1 <- round(modsum\$coefficients[2,4],3)
  my.p2 <- round(modsum\$coefficients[3,4],3)
  pval <- bquote(
    italic(p-value1) == .(format(my.p1)) *","
    ~~italic(p-value2) == .(format(my.p2)) *","
    ~~ r^2 == .(r2))
  mtext(pval, side =3, col ="darkblue", cex=0.6, font=2)

  fit_mcmc <- MCMCregress(gcu_usage ~ days, data= d, burnin = 1000, mcmc = 10000, thin = 1)
  rn <- summary(fit_mcmc)
  lower_credability <- round(rn\$quantiles[2,c(1)],1)
  bayesian_median <- round(rn\$quantiles[2,c(3)],1)
  upper_credability <- round(rn\$quantiles[2,c(5)],1)

  newd <- data.frame(days = c(max(d\$days)+30, max(d\$days)+60, max(d\$days)+90))
  pred.w.clim <- predict(fit, newd, interval = "confidence")
  latest <- (d[ which(d\$days == max(d\$days)),][1,4])
  month <- round((pred.w.clim[1,1] - latest)/latest,2)*100
  two_months <- round((pred.w.clim[2,1] - latest)/latest,2)*100
  three_months <- round((pred.w.clim[3,1] - latest)/latest,2)*100
  matplot(newd\$days, pred.w.clim,
      main = "QD Forecast",
      lty = c(1,2,2), type = "l", lwd =c(1.5,1,1),
      ylab = '',
      col = c("cadetblue4","chocolate4","chocolate4"),
      xlab = '',
      xaxt='n',
      cex=0.8,
      cex.axis=0.8)
  grid (lty = 6, col = "cornsilk2")
  legend("topleft", col = c("chocolate4"), lty = 2,
    legend = "95% Confidence interval",
    cex=0.8,
    bty = "n")
  increases <- bquote(italic(month) == .(format(month))*italic("%")*","
    ~~italic(twomonths) == .(format(two_months))*italic("%")* ","
    ~~italic(threemonths) == .(format(three_months))*italic("%"))
  mtext(increases, side =1, col ="darkblue", cex=0.6, font=2)
  bayesian <- bquote(italic(Bayes) == .(format(bayesian_median))*","
    ~~italic(low) == .(format(lower_credability))* ","
    ~~italic(up) == .(format(upper_credability)))
  mtext(bayesian, side =3, col ="darkblue", cex=0.6, font=2)

  d <- cells[ which(cells\$cluster=='yl'), ]
  d <- d[order(d\$days) , ]

  fit <- lm(gcu_usage ~ days + I(days^2), data=d)
  plot(d\$days, d\$gcu_usage,
    main = "YL GCU usage",
    xlab = "",
    ylab = "GCU per unit",
    col = "darkred",
    pch=16,
    cex=0.8,
    cex.axis=0.8)
  grid(lty = 6, col = "cornsilk2")
  axis(1, at = d\$days,
    labels = paste("\n", d\$date ),
    padj = 1,
    col.axis="darkred",
    cex.axis=0.7,
    tck=0)
  lines(d\$days, predict(fit),col = "darkblue",lwd=1.5)
  r2 <- round(summary(fit)\$r.squared, 2)
  modsum <- summary(fit)
  my.p1 <- round(modsum\$coefficients[2,4],3)
  my.p2 <- round(modsum\$coefficients[3,4],3)
  pval <- bquote(
    italic(p-value1) == .(format(my.p1)) *","
    ~~italic(p-value2) == .(format(my.p2)) *","
    ~~ r^2 == .(r2))
  mtext(pval, side =3, col ="darkblue", cex=0.6, font=2)

  fit_mcmc <- MCMCregress(gcu_usage ~ days, data= d, burnin = 1000, mcmc = 10000, thin = 1)
  rn <- summary(fit_mcmc)
  lower_credability <- round(rn\$quantiles[2,c(1)],1)
  bayesian_median <- round(rn\$quantiles[2,c(3)],1)
  upper_credability <- round(rn\$quantiles[2,c(5)],1)

  newd <- data.frame(days = c(max(d\$days)+30, max(d\$days)+60, max(d\$days)+90))
  pred.w.clim <- predict(fit, newd, interval = "confidence")
  latest <- (d[ which(d\$days == max(d\$days)),][1,4])
  month <- round((pred.w.clim[1,1] - latest)/latest,2)*100
  two_months <- round((pred.w.clim[2,1] - latest)/latest,2)*100
  three_months <- round((pred.w.clim[3,1] - latest)/latest,2)*100
  matplot(newd\$days, pred.w.clim,
      main = "YL Forecast",
      lty = c(1,2,2), type = "l", lwd =c(1.5,1,1),
      ylab = '',
      col = c("cadetblue4","chocolate4","chocolate4"),
      xlab = '',
      xaxt='n',
      cex=0.8,
      cex.axis=0.8)
  grid (lty = 6, col = "cornsilk2")
  legend("topleft", col = c("chocolate4"), lty = 2,
    legend = "95% Confidence interval",
    cex=0.8,
    bty = "n")
  increases <- bquote(italic(month) == .(format(month))*italic("%")*","
    ~~italic(twomonths) == .(format(two_months))*italic("%")* ","
    ~~italic(threemonths) == .(format(three_months))*italic("%"))
  mtext(increases, side =1, col ="darkblue", cex=0.6, font=2)
  bayesian <- bquote(italic(Bayes) == .(format(bayesian_median))*","
    ~~italic(low) == .(format(lower_credability))* ","
    ~~italic(up) == .(format(upper_credability)))
  mtext(bayesian, side =3, col ="darkblue", cex=0.6, font=2)

  dev.off()

EOF

  echo ''| sendgmr --subject="Footptint GABE on ${date}" \
  --attachment_files="${pdf}"  --to=aredakov@google.com,joshuak@google.com,quanwang@google.com

}

footptint_gabe
