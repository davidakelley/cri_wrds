# Plot fitting errors of Carhart 4 factors
#
# David Kelley, 2018-2019

library(tidyverse)
library(tis)
library(lubridate)
library(modelr)
library(zoo)
library(ggpubr)
library(readr)
library(stringi)
library(readxl)
library(stargazer)
library(tsDyn)
library(DescTools)

FLAG_PULL_DATA = TRUE
timeStart = Sys.time()

# Get data #####################################################################
if (FLAG_PULL_DATA) {
  LIMIT_DATA = -1

  # Source script to generate connection to WRDS PSQL server
  source("open_wrds.R")

  # Get factor estimates
  cat("Fetching factor data... ")
  res = dbSendQuery(wrds, "select * from ff_all.factors_monthly")
  raw_factors = dbFetch(res, n = LIMIT_DATA) %>% as_tibble()
  day(raw_factors$date) = days_in_month(raw_factors$date)

  res = dbSendQuery(wrds, "select * from ff_all.liq_ps")
  raw_ps_factors = dbFetch(res, n = LIMIT_DATA) %>% as_tibble()
  day(raw_ps_factors$date) = days_in_month(raw_ps_factors$date)
  cat("Done.\n")

  factors_crsp = raw_factors %>%
    merge(., raw_ps_factors, by="date", all=TRUE) %>%
    select(date, mktrf, hml, smb, umd, ps_vwf, rf) %>%
    arrange(date) %>%
    filter(ps_vwf != -99, !is.na(ps_vwf)) %>%
    rename(liq = ps_vwf) %>%
    as_tibble()

  # Get ratings data
  cat("Fetching ratings data... ")
  ratings_raw = dbFetch(
    dbSendQuery(wrds,
                "select DISTINCT company_id, rdate, rating
                 from wrds_erating
                 where rtype = 'Local Currency LT'"),
    n=-1)

  # Moody's ratings data
  ratings_fisd = dbFetch(
    dbSendQuery(wrds,
                "SELECT issue_id, issue_id, rating_date, rating
                FROM fisd_ratings
                where rating_type='MR'"),
    n=-1)

  info_fisd = dbFetch(
    dbSendQuery(wrds,
                "SELECT issuer_id, cusip_name, industry_group, country_domicile
                FROM fisd_issuer"),
    n=-1)

  ratings_match = dbFetch(
    dbSendQuery(wrds, "select DISTINCT company_id, gvkey
                      from wrds_enames
                      where gvkey is not NULL"),
    n=-1)


  crsp_match = dbFetch(
    dbSendQuery(wrds, "select DISTINCT gvkey, lpermco
                      from ccmxpf_linktable
                      where lpermco is not NULL"),
    n=-1) %>%
    rename(permco = lpermco) %>%
    as_tibble()

  # Get Compustat company industry
  res = dbSendQuery(wrds, "select GVKEY, GSECTOR from comp.company")
  industry_info = dbFetch(res, n=-1) %>%
    dplyr::filter(!is.na(gsector))

  match_table = ratings_match %>%
    merge(., crsp_match, by="gvkey") %>%
    merge(., industry_info, by="gvkey")

  ratings = ratings_raw %>%
    merge(., match_table, by="company_id") %>%
    mutate(date = lubridate::ymd(rdate)) %>%
    filter(gsector < 40 | gsector > 49) %>%
    filter(gsector != 55) %>%
    group_by(permco, date) %>%
    filter(rdate == max(rdate)) %>%
    mutate(ig = case_when(
      substr(rating, 1, 1) == 'A' ~ "invest",
      substr(rating, 1, 3) == 'BBB' ~ "invest",
      substr(rating, 1, 2) == 'BB' ~ "speculative",
      substr(rating, 1, 1) == 'B'  ~ "speculative",
      TRUE ~ "junk")) %>%
    ungroup() %>%
    select(date, permco, ig, rating) %>%
    arrange(permco, date)
  day(ratings$date) = days_in_month(ratings$date)
  cat("Done.\n")

  # Get data on which exchange stocks trade on - we'll only use NYSE stocks

  res <- dbSendQuery(wrds, "select DISTINCT PERMCO, PERMNO, SHRCD, EXCHCD, TICKER
                     from CRSP.MSE
                     where SHRCD is not NULL")
  return_info_all = dbFetch(res)
  # We're going to filter on common stocks (shrcd == 10 or 11) and those that
  # trade on NYSE, American Stock Exchange or Nasdaq (exchcd == 1, 2 or 3)
  permno_common = return_info_all %>%
    filter(shrcd == 10 | shrcd == 11, exchcd == 1 | exchcd == 2 | exchcd == 3)

  # Get firm-level data
  cat("Fetching return data... ")
  res <- dbSendQuery(wrds, "select DATE, PERMNO, PERMCO, SHROUT, PRC, RET
                     from CRSP.MSF
                     where PRC is not null AND RET is not NULL")
  crsp_data <- dbFetch(res, n = -1)
  cat("Done.\n")
  return_data = crsp_data %>%
    filter(permno %in% permno_common$permno) %>% # Filter on common stocks
    filter(prc <= 500) %>% # MFA condition - I don't know why you would do this (DK).
    mutate(meq = shrout * abs(prc)) %>% # me for each permno
    group_by(date, permco) %>% # to calc market cap, merge permnos with same permnco
    mutate(mktval = sum(meq)/1000) %>%
    arrange(date, permco, desc(meq)) %>%
    group_by(date, permco) %>%
    slice(1) %>% # keep only permno with largest meq
    ungroup() %>%
    select(-one_of(c("shrout", "prc", "meq"))) %>%
    filter(year(date) > 1972) %>%
    arrange(permco, date) %>%
    as_tibble()
  day(return_data$date) = days_in_month(return_data$date)
  cat("Done.\n")

  # Compile into firm-level data
  firm_data = return_data %>%
    merge(., ratings, by=c("date", "permco"), all=TRUE) %>%
    select(date, permco, ret, mktval, ig) %>%
    arrange(permco, date) %>%
    group_by(permco) %>%
    mutate_at(vars(ig), funs(na.locf(., na.rm = FALSE))) %>%
    ungroup() %>%
    replace_na(list(ig="unrated")) %>%
    arrange(permco, date)

  firm_data$mktval = as.numeric(firm_data$mktval)
  firm_data$ret = as.numeric(firm_data$ret)

  # Save raw data
  save(firm_data, factors_crsp, file="mfa_replication_data.RData")

} else {
  load("mfa_replication_data.RData")
}

# See http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html
# Pick the Fama/French 5 Factors (2x3) CSV file
factors_french = read_csv("data/F-F_Research_Data_5_Factors_2x3.CSV",
                          skip=3) %>%
  mutate(dateM = ymd(paste(stri_sub(X1, 1, 4), stri_sub(X1, 5,6), "01", sep='-')),
         date = dateM + days_in_month(dateM)-1) %>%
  rename(mktrf = `Mkt-RF`) %>%
  rename_all(tolower) %>%
  select(date, mktrf, smb, hml, rmw, cma) %>%
  mutate_if(is.character, as.numeric) %>%
  mutate_at(vars(-date), function(x) x / 100)

# AQR Quality minus junk (Asness, Frazzini & Pedersen, 2014) and
# momentum factors
# See https://www.aqr.com/Insights/Datasets/Quality-Minus-Junk-Factors-Monthly
qmj_data <- read_excel("data/Quality Minus Junk Factors Monthly.xlsx",
                       sheet = "QMJ Factors",
                       skip=18)
qmj = qmj_data %>%
  mutate(date = mdy(DATE),
         qmj = USA) %>%
  select(date, qmj)

umd_data <- read_excel("data/Quality Minus Junk Factors Monthly.xlsx",
                       sheet = "UMD",
                       skip=18)
umd = umd_data %>%
  mutate(date = mdy(DATE),
         umd = USA) %>%
  select(date, umd)

liq = factors_crsp %>%
  select(date, liq, rf)

# Merge factors together
factors = factors_french %>%
  merge(umd, by='date') %>%
  merge(qmj, by='date') %>%
  merge(liq, by='date') %>%
  as_tibble()


# Run portfolio regressions ####################################################
firm_count = firm_data %>%
  filter(year(date) > 1972, year(date) < 2018) %>%
  select(ig, permco) %>% distinct() %>% group_by(ig) %>%
  summarise(count=n()) %>%
  ungroup() %>%
  arrange(match(ig, c("invest", "speculative", "junk", "unrated")))

portfolios = firm_data %>%
  filter(year(date) > 1972, year(date) < 2018) %>%
  mutate(ret = Winsorize(ret, probs = c(0.01, 0.99), na.rm=TRUE)) %>%
  group_by(date, ig) %>%
  summarise(retmean = weighted.mean(ret, mktval, na.rm=TRUE)) %>%
  ungroup() %>%
  mutate(mdate = as.yearmon(date)) %>%
  group_by(ig) %>%
  arrange(date) %>%
  merge(factors_crsp, by='date') %>%
  ungroup() %>%
  select(mdate, ig, retmean, rf) %>%
  group_by(ig) %>%
  mutate(ret = retmean - rf,
         cret = cumsum(log1p(retmean)))

#
factors_clean = factors %>%
  mutate(mdate = as.yearmon(date))

# Regression equation for predicted returns:
mdlEquation = ret ~ mktrf + smb + hml + rmw + cma + umd + qmj

# Create regression table
mdlIG = portfolios %>%
  filter(ig == 'invest') %>%
  merge(., factors_clean, by="mdate") %>%
  lm(mdlEquation, .)
mdlSG = portfolios %>%
  filter(ig == 'speculative') %>%
  merge(., factors_clean, by="mdate") %>%
  lm(mdlEquation, .)
mdlJG = portfolios %>%
  filter(ig == 'junk') %>%
  merge(., factors_clean, by="mdate") %>%
  lm(mdlEquation, .)
mdlUR = portfolios %>%
  filter(ig == 'unrated') %>%
  merge(., factors_clean, by="mdate") %>%
  lm(mdlEquation, .)
mdlDiff = portfolios %>%
  filter(ig == 'speculative' | ig == 'invest') %>%
  group_by(mdate) %>%
  mutate(ret = diff(ret)) %>%
  slice(1) %>%
  ungroup() %>%
  merge(., factors_clean, by="mdate") %>%
  lm(mdlEquation, .)

stargazer(mdlIG, mdlSG, mdlJG, mdlUR, mdlDiff,
          type='text',
          dep.var.caption  = "Portfolio Regressions",
          dep.var.labels.include = FALSE,
          column.labels = c("Investment", "Speculative",
                            "CCC or lower", "Unrated", "SMI"),
          add.lines = list(c("Total Firms", firm_count$count)),
          out='output/reg_table_mfa_replicate.html')
openHTML <- function(x) browseURL(paste0('file://', file.path(getwd(), x)))
openHTML('output/reg_table_mfa_replicate.html')

## Calculating index
# Get expected returns from each
expected = portfolios %>%
  merge(., factors_clean, by="mdate") %>%
  group_by(ig) %>%
  nest() %>%
  mutate(model = map(data, ~lm(mdlEquation, data=.)),
         fitted = map2(data, model, add_predictions),
         const = map2(model, data, function(m1, d1) {rep(m1[[1]]['(Intercept)'], dim(d1)[1])})) %>%
  unnest(fitted, const) %>%
  select(mdate, ig, pred, const) %>%
  group_by(ig) %>%
  mutate(ret_pred = cumsum(log1p(pred - const))) %>%
  select(mdate, ig, ret_pred) %>%
  spread(ig, ret_pred)

# MFA cointegrating vector estimation (same as using VECM package)
mdl_cum = lm(invest ~ speculative, expected[,c("invest", "speculative")])
mfa_fci = tibble(mdate = expected$mdate,
                    fci = scale(as.vector(mdl_cum$residuals)))

################################################################################
# Plots

std_theme = theme_minimal() + theme(axis.title.x=element_blank())

# Cumulative Returns
plot_actual_returns = portfolios %>%
  ggplot() +
  geom_line(aes(x=mdate, y=cret, color=ig)) +
  scale_color_discrete(name="Rating",
                       breaks=c("invest", "speculative", "junk", "unrated"),
                       labels=c("Investment", "Speculative", "Junk", "Unrated")) +
  ggtitle("Cumulative Realized Returns") +
  ylab("Cumulative Return") +
  std_theme
# print(plot_actual_returns)
# ggsave(plot_actual_returns, file="output/returns_actual.pdf")

# Expected Returns
plot_expected_returns = expected %>%
  gather(ig, ret_pred, -mdate) %>%
  ggplot() +
  geom_line(aes(x=mdate, y=ret_pred, color=ig)) +
  scale_color_discrete(name="Rating",
                       breaks=c("invest", "speculative"),
                       labels=c("Investment", "Speculative")) +
  ggtitle("Cumulative Expected Returns") +
  ylab("Cumulative Return") +
  std_theme
# print(plot_expected_returns)
# ggsave(plot_expected_returns, file="output/returns_expected.pdf")

# Scaled Expected Returns
plot_cum_pred_returns = expected %>%
  mutate_at(vars(-mdate), function(x) scale(x)) %>%
  select(mdate, invest, speculative) %>%
  gather(ig, ret_pred, invest, speculative) %>%
  ggplot() +
  geom_line(aes(x=mdate, y=ret_pred, color=ig)) +
  scale_color_discrete(name="Rating",
                       breaks=c("invest", "speculative"),
                       labels=c("Investment", "Speculative")) +
  ggtitle("A1: Cumulative Expected Returns") +
  ylab("Normalized") +
  std_theme
# print(plot_cum_pred_returns)
# ggsave(plot_cum_pred_returns, file="output/returns_normalized.pdf")

# FCI
plot_mfa_fci = mfa_fci %>%
  ggplot() +
  geom_line(aes(x=mdate, y=-fci)) +
  std_theme +
  ylab("FCI") +
  ggtitle('Figure 1: MFA-FCI Index')
# print(plot_mfa_fci)
# ggsave(plot_mfa_fci, file="output/mfa_fci.pdf")


plot_progress = ggarrange(plot_actual_returns, plot_expected_returns,
                        plot_cum_pred_returns, plot_mfa_fci,
                        common.legend=TRUE, legend='bottom') %>%
  annotate_figure(top=text_grob("MFA-FCI Replication Progress", face = "bold", size = 16))
print(plot_progress)
ggsave(plot_progress, file="output/mfa_replication_progress.pdf",
       width=10, height=7)

timeEnd = Sys.time()
cat("Elapsed time:\n")
print(timeEnd - timeStart)

