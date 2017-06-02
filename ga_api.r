R_GA_API
# https://developers.google.com/oauthplayground/
# https://code.google.com/p/r-google-analytics/
# https://developers.google.com/analytics/devguides/reporting/core/dimsmets#mode=web

library(RCurl)
library(RJSONIO)
source("./RGoogleAnalytics.R")
source("./QueryBuilder.R")

# Step: 1. Authorize your account and paste the accesstoken
query <- QueryBuilder()
access_token <- query$authorize()

# Step: 2. Create a new Google Analytics API object
ga <- RGoogleAnalytics()
ga.profiles <- ga$GetProfileData(access_token)

profile <- "ga:676522"

# Step: 3. Build the query string, use the profile by setting its index value
query$Init(start.date = "2014-01-01",
    end.date = "2014-01-28",
    dimensions = "ga:dayOfWeek, ga:hour",
    metrics = "ga:visits",
    sort = "ga:dayOfWeek, ga:hour",
    max.results = 1000,
    table.id = profile,
    access_token = access_token)

# Step: 4. Make a request to get the data from the API
ga.data <- ga$GetReportData(query)

# Step: 5. Look at the returned data
head(ga.data)
