# Forming Fama-French factors

library(zoo)
library(tidyverse)
library(ggpubr)
library(lubridate)
library(modelr)

FLAG_PULL_DATA = FALSE
timeStart = Sys.time()
# Constant to multiply by to go from period returns to annual returns
CONST_ANNUALIZE = 252

################################################################################
# Query
if (FLAG_PULL_DATA) {
  # Source script to generate connection to WRDS PSQL server
  source("open_wrds.R")

  # Get Compustat data
  cat("Fetching Compustat data... ")
  res = dbSendQuery(wrds, "select GVKEY, DATADATE, PIFO, PI
                    from COMP.FUNDA
                    where INDFMT='INDL' and DATAFMT='STD' and CONSOL='C' and POPSRC='D'")
  compustat_data <- dbFetch(res, n = -1)
  compustat_clean = compustat_data %>%
    mutate(Date = as.Date(datadate))
  rm(compustat_data)
  cat("Done.\n")

  # Get data on which exchange stocks trade on - we'll only use NYSE stocks
  cat("Fetching exchange data... ")
  res <- dbSendQuery(wrds, "select DISTINCT PERMNO
                     from CRSP.MSE where exchcd = 1")
  return_info <- dbFetch(res, n = -1)
  cat("Done.\n")

  # Get CRSP price data
  cat("Fetching CRSP data... ")
  res <- dbSendQuery(wrds, "select DATE, PERMNO, PERMCO, SHROUT, PRC, RET
                     from CRSPM.DSF
                     where PRC is not null")
  crsp_data <- dbFetch(res, n = -1)
  crsp_clean = crsp_data %>%
    filter(!is.na(prc), permno %in% return_info$permno) %>%
    mutate(Date = as.Date(date),
           ymDate = as.yearmon(Date)) %>%
    select(-date) %>%
    mutate(meq = shrout * abs(prc)) %>% # me for each permno
    group_by(Date, permco) %>%
    mutate(ME = sum(meq)/1000) %>% # to calc market cap, merge permnos with same permnco
    arrange(Date, permco, desc(meq)) %>%
    group_by(Date, permco) %>%
    slice(1) %>% # keep only permno with largest meq
    ungroup() %>%
    select(-one_of(c("shrout", "prc"))) %>%
    arrange(permno, Date)
  rm(crsp_data)
  cat("Done.\n")

  # Link table between CRSP and Compustat
  cat("Fetching link data... ")
  res <- dbSendQuery(wrds,"select GVKEY, LINKTYPE, LINKPRIM, LPERMCO
                   from crsp.ccmxpf_lnkhist")
  link_data <- dbFetch(res, n = -1)
  cat("Done.\n")

  link_clean = link_data %>%
    filter(linktype %in% c("LU", "LC", "LS")) %>%
    filter(linkprim %in% c("P", "C", "J")) %>%
    rename(permco = lpermco)
  rm(link_data)

  cat("Merging data... ")
  combined = link_clean %>%
    merge(., compustat_clean, by="gvkey", all=TRUE) %>%
    merge(., crsp_clean, by=c("Date", "permco"), all=TRUE) %>%
    select(-one_of(c("gvkey", "linktype", "linkprim", "datadate"))) %>%
    arrange(permno, Date) %>%
    as_tibble()
  # rm(compustat_clean, crsp_clean, link_clean)
  cat("Done.\n")

  # Get factor estimates
  cat("Fetching factor data... ")
  res = dbSendQuery(wrds, "select * from ff_all.factors_daily")
  factors = dbFetch(res, n = -1) %>%
    rename(Date = date) %>%
    as_tibble()
  cat("Done.\n")

  # Save raw data
  save(combined, factors, file="ff_foreign_data_daily.RData")
  rm(res, wrds)

} else {
  load("ff_foreign_data_daily.RData")
  combined$Date = as.Date(combined$Date)
  factors$Date = as.Date(factors$Date)
}

combined_filled = combined %>%
  group_by(permco) %>%
  fill(pifo, pi)

combined_clean = combined_filled %>%
  filter(Date >= dmy("1/1/1985"), !is.na(ME), !is.na(ret))

# functions ====================================================================
cut_quantile = function(x, n=4) {
  break_points = quantile(x, na.rm = TRUE, probs = seq(0,1,1/n))
  if (length(unique(break_points)) < length(break_points)) {
    return(rep(NA, length(x)))
  } else {
    return(as.numeric(cut(x, breaks = break_points, include.lowest = TRUE)))
  }
}


slice_portfolios = function(df, date_var, slice_var, weight_var, ret_var, n_slice = 4) {
  # Slice returns into n portfolios based on a slice_var, weighted by weight_var
  # Outputs portfolio returns for each period.

  portfolio_splits = df %>%
    filter(!is.na(!!date_var)) %>%
    select(!!date_var, !!slice_var, !!weight_var, !!ret_var) %>%
    group_by(!!date_var) %>%
    mutate(quantlabel = cut_quantile(!!slice_var, n_slice))

  portfolio_rets = portfolio_splits %>%
    filter(!is.na(quantlabel)) %>%
    group_by(!!date_var, quantlabel) %>%
    summarize(port_ret = weighted.mean(!!ret_var, !!weight_var, na.rm=TRUE)) %>%
    ungroup() %>%
    mutate(portlabel = paste(as.character(slice_var), quantlabel, sep="_")) %>%
    select(-quantlabel) %>%
    spread(portlabel, port_ret) %>%
    arrange(!!date_var)
  portfolio_rets[is.na(portfolio_rets)] = 0

  return(portfolio_rets)
}

portfolio_regressions = function(df, date_var, slice_var, weight_var, ret_var, n_slice = 4) {

  portfolio_rets = slice_portfolios(df, date_var, slice_var, weight_var, ret_var, n_slice = 4)

  # Get residuals from regressions on factors:
  resid_foreign_inc = portfolio_rets %>%
    gather(key=port, value=port_ret, -Date) %>%
    group_by(port) %>%
    merge(factors, by="Date") %>%
    nest() %>%
    mutate(model = map(data, ~lm(port_ret ~ mktrf + hml + smb + umd, data=.)),
           resid = map2(data, model, add_residuals)) %>%
    unnest(resid) %>%
    select(Date, port, resid) %>%
    spread(key=port, value=resid)
  return(resid_foreign_inc)
}


make_factor = function(resids) {
  # Weight the residuals by the ex-post average return

  resid_melt = resids %>%
    gather('Group', 'ret', -Date)

  weights = resid_melt %>%
    group_by(Group) %>%
    summarize(retavg = mean(ret)) %>%
    ungroup() %>%
    mutate(weight = retavg / max(retavg)) %>%
    select(Group, weight)

  factor_ret = resid_melt %>%
    merge(weights, by="Group") %>%
    group_by(Date) %>%
    summarize(f_ret = sum(weight * ret)) %>%
    ungroup() %>%
    mutate(factor = f_ret - mean(f_ret)) %>%
    select(Date, factor)

  return(factor_ret)
}

plot_portfolios = function(portrets, split_name) {
  # Make combined plot of average return by group and cumulative returns by group

  cum_rets = portrets %>%
    gather('group_name', 'ret', -Date) %>%
    mutate(Group = as.numeric(unlist(regmatches(group_name, gregexpr("[[:digit:]]+", group_name))))) %>%
    group_by(group_name) %>%
    mutate(cumret = cumprod(1+ret)-1,
           cumret2 = cumsum(ret)) %>% # Note cumsum, not cumulative returns
    as_tibble()

  plot_cum_ret = cum_rets %>%
    ggplot(aes(x=Date,y=cumret,color=as.factor(Group))) +
    geom_line() +
    ggtitle("Cumulative Excess Return") +
    xlab("") +
    ylab("") +
    theme_minimal()

  plot_avg_ret = cum_rets %>%
    group_by(Group) %>%
    summarize(retavg = mean(ret * CONST_ANNUALIZE)) %>%
    ggplot(aes(x=Group, y=retavg, fill=as.factor(Group))) +
    geom_col() +
    xlab(paste(split_name, "Group")) +
    ylab("") +
    ggtitle("Average Annual Excess Return") +
    theme_minimal()

  plot_combined = ggarrange(plot_avg_ret, plot_cum_ret,
                            common.legend=FALSE, legend="none") %>%
    annotate_figure(top=text_grob(paste(split_name, "Portfolios"),
                                  face = "bold", size = 16))
  return(plot_combined)
}


## Foreign income ##############################################################
combined_filtered = combined_clean %>%
  mutate(pctForInc = pifo/pi) %>%
  filter(!is.na(pctForInc))

ports_fi = slice_portfolios(combined_filtered, sym("Date"), sym("pctForInc"), sym("ME"), sym("ret"), n=10)

port_foreign_inc = portfolio_regressions(combined_filtered, sym("Date"), sym("pctForInc"), sym("ME"), sym("ret"), n=10)
plot_foreign_ports = plot_portfolios(port_foreign_inc, 'Foreign Income')
print(plot_foreign_ports)
ggsave(plot_foreign_ports, file='foreign_income_portfolios.pdf')


factor_foreign = make_factor(port_foreign_inc)
plot_factor_qret = factor_foreign %>%
  mutate(fAnnual = rollapply(factor, CONST_ANNUALIZE/4, function(x) prod(1+x)-1, fill=NA)) %>%
  ggplot(aes(x=Date,y=fAnnual)) +
  geom_line() +
  xlab("Date") +
  ylab("One-Quarter Return") +
  theme_minimal()

plot_foreign_cum = factor_foreign %>%
  mutate(cret = cumprod(1+factor)-1) %>%
  ggplot(aes(x=Date,y=cret)) +
  geom_line() +
  xlab("Date") +
  ylab("Cumulative Return") +
  theme_minimal()

plot_factor = ggarrange(plot_factor_qret, plot_foreign_cum,
                        common.legend=FALSE, legend="none") %>%
  annotate_figure(top=text_grob("Foreign Income Factor", face = "bold", size = 16))

print(plot_factor)
ggsave(plot_factor, file='foreign_income_factor.pdf')


## Report timing
timeEnd = Sys.time()
print("Elapsed time:\n")
print(timeEnd - timeStart)
