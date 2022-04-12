# Fama-French & Momentum factor indexes
#
# David Kelley, 2018

library(tidyverse)
library(lubridate)
library(tis)

# Get data
source("open_wrds.R")

res = dbSendQuery(wrds, "select * from ff_all.factors_monthly")
factors = dbFetch(res, n=-1) %>% as_tibble() %>%
  select(date, mktrf, hml, smb, umd)

# Save- we'll use this as a comparison when creating our factors
save(factors, file="ff_factors.RData")

# Plot
ggplot(filter(factors, year(date) > 1970), aes(x=date,y=smb)) + geom_line()



# Cumulative risk indexes
make_risk_index <- function(factor_return) {
  # Get the non-nan sample so we can make a cumulative return
  factor_return[is.na(factor_return)] = 0

  cumulative = cumprod(1+factor_return-mean(factor_return))-1
  result = cumulative - mean(cumulative)

  return(result)
}

cum_ret = factors %>%
  filter(year(date) >= 1970) %>%
  gather(factor, value, -date) %>%
  group_by(factor) %>%
  mutate(cvalue = make_risk_index(value)) %>%
  select(-value) %>%
  ungroup() %>%
  spread(factor, cvalue)



factor_plot = factors %>%
  filter(year(date) >= 1970) %>%
  gather(factor, value, -date) %>%
  ggplot(aes(x=date,y=value,color=factor)) + geom_line()
factor_plot = nberShade(factor_plot) + xlim(as.Date("1970-01-01"), Sys.Date())
print(factor_plot)

risk_plot1 = cum_ret %>%
  gather(factor, value, -date) %>%
  ggplot(aes(x=date,y=value,color=factor)) + geom_line()
risk_plot = nberShade(risk_plot1) + xlim(as.Date("1970-01-01"), Sys.Date())
plot(risk_plot)
