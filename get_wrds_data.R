# Data script for WRDS cloud
#
# WRDS connection set up in .Rprofile.
#
# tidyverse not supported, most load components separately. Done in .Rprofile.
# There's a lot of places where dplyr:: is used to make sure we're getting the
# right function since we're not loading tidyverse

library(tidyverse);
library(zoo)
require(data.table); library(lubridate)

################################################################################
### Set up WRDS Connection
# If we were going to set up the WRDS connection, this is how its done:
source('open_wrds.R')

################################################################################
### LOAD COMPUSTAT FROM WRDS ###
# Downloads Compustat and Compustat/Crsp merging link from WRDS
# adds the linked permno to the compustat dataset
# no filtering except must have PERMNO link

# res <- dbSendQuery(wrds,
# "select name from dictionary.columns where libname='COMPM' and memname='FUNDQ'")

# retrieve Compustat annual data (takes 10mins each below)
# STD is unrestatd data
res <- dbSendQuery(wrds,"select GVKEY, CUSIP, DATADATE, FYR, FYEAR, SICH, NAICSH,
                   AT, LT, SEQ, CEQ, PSTKL, PSTKRV, PSTK, TXDITC, TXDB, ITCB,
                   REVT, COGS, XINT, XSGA, IB, TXDI, DVC, ACT, CHE, LCT,
                   DLC, TXP, DP, PPEGT, INVT
                   from COMP.FUNDA
                   where INDFMT='INDL' and DATAFMT='STD' and
                   CONSOL='C' and POPSRC='D'")
# n=-1 denotes no max but retrieve all record
data.comp.funda <- dbFetch(res, n = -1)
save(data.comp.funda, file = "data.comp.funda.RData")

# retrieve Compustat quarterly data
# STD is unrestatd data
res <- dbSendQuery(wrds,"select GVKEY, CUSIP, DATADATE, FYR, FYEARQ,
                   ATQ, LTQ, SEQQ, CEQQ, PSTKRQ, PSTKQ, TXDITCQ, TXDBQ,
                   REVTQ, COGSQ, XINTQ, XSGAQ, IBQ, TXDIQ, ACTQ, CHEQ, LCTQ,
                   DLCQ, TXPQ, DPQ, PPEGTQ, INVTQ, EPSPXQ, RDQ
                   from COMPM.FUNDQ
                   where INDFMT='INDL' and DATAFMT='STD' and
                   CONSOL='C' and POPSRC='D'")
# n=-1 denotes no max but retrieve all record
data.comp.fundq <- dbFetch(res, n = -1)
save(data.comp.fundq, file = "data.comp.fundq.RData")

# retrieve Merged Compustat/CRSP link table
res <- dbSendQuery(wrds,"select GVKEY, LPERMNO, LINKDT, LINKENDDT, LINKTYPE, LINKPRIM
                   from crsp.ccmxpf_lnkhist")
data.ccmlink <- dbFetch(res, n = -1)
save(data.ccmlink, file = "data.ccmlink.RData")

# Merge the linked Permno onto Compustat dataset
# compared to SAS code based on WRDS FF Research macro, I don't include all
#   Linktypes but add J Linkprim
# including J linkprim is key bc/ allows me to get the post-2010 Berkshire history
# omitting non-primary linktypes led to 1% fewer obs (2,000) but cleaner data
#   (datadate<="2013-12-31" for comparability)

# use only primary links (from WRDS Merged Compustat/CRSP examples)
# remove compustat fiscal ends that do not fall within linked period;
#   linkenddt=NA (from .E) means ongoing
# prioritize linktype, linkprim based on order of preference/primary if duplicate
# inner join, keep only if permno exists

data.ccm <-  data.ccmlink %>%
  dplyr::filter(linktype %in% c("LU", "LC", "LS")) %>%
  dplyr::filter(linkprim %in% c("P", "C", "J")) %>%
  merge(data.comp.funda, by="gvkey") %>%
  dplyr::mutate(datadate = as.Date(datadate),
                permno = as.factor(lpermno),
                linkdt = as.Date(linkdt),
                linkenddt = as.Date(linkenddt),
                linktype = factor(linktype, levels=c("LC", "LU", "LS")),
                linkprim = factor(linkprim, levels=c("P", "C", "J"))) %>%
  dplyr::filter(datadate >= linkdt & (datadate <= linkenddt | is.na(linkenddt))) %>%
  dplyr::arrange(datadate, permno, linktype, linkprim) %>%
  dplyr::distinct(datadate, permno, .keep_all = TRUE)
save(data.ccm, file = "data.ccm.RData")
rm(data.comp.funda, data.ccmlink)

################################################################################
### COMPUSTAT CLEANING AND VAR CALC ###

load("data.ccm.RData")
data.comp <- data.ccm %>%
  rename(PERMNO=permno) %>% data.table %>% # ensure col names match crsp's
  group_by(PERMNO) %>%
  mutate(datadate = as.yearmon(datadate)) %>%
  # number of years in data; future option to cut first year data; works but leads to warnings
  # tests based on BE spread show FF no longer impose this condition (even though mentioned in FF'93)
  ungroup %>% arrange(datadate, PERMNO) %>% data.frame %>%
  distinct(datadate, PERMNO, .keep_all = TRUE) # hasn't been issue but just in case

data.comp.a <- data.comp %>%
  group_by(PERMNO) %>%
  # consistent w/ French website variable definitions
  mutate(BE = coalesce(seq, ceq + pstk, at - lt) + coalesce(txditc, txdb + itcb, 0) -
           coalesce(pstkrv, pstkl, pstk, 0),
         OpProf = (revt - coalesce(cogs, 0) - coalesce(xint, 0) - coalesce(xsga,0)),
         # FF condition
         OpProf = as.numeric(ifelse(is.na(cogs) & is.na(xint) & is.na(xsga), NA, OpProf)),
         GrProf = (revt - cogs),
         # operating; consistent w/ French website variable definitions
         Cflow = ib + coalesce(txdi, 0) + dp,
         Inv = (coalesce(ppegt - lag(ppegt), 0) + coalesce(invt - lag(invt), 0)) / lag(at),
         # note that lags use previously available (may be different from 1 yr)
         AstChg = (at - lag(at)) / lag(at)
  ) %>% ungroup %>%
  dplyr::arrange(datadate, PERMNO) %>%
  dplyr::select(datadate, PERMNO, at, revt, ib, dvc, BE:AstChg) %>%
  # replace Inf w/ NA's
  dplyr::mutate_if(is.numeric, funs(ifelse(is.infinite(.), NA, .))) %>%
  # round to 5 decimal places (for some reason, 0's not properly coded in some instances)
  dplyr::mutate_if(is.numeric, funs(round(., 5)))
save(data.comp.a, file="data.comp.a.RData")
rm(data.ccm, data.comp)

################################################################################
### LOAD CRSP FROM WRDS ###
# Downloads CRSP MSE, MSF, and MSEDELIST tables from WRDS
# merges, cleans, and for market cap calc, combines permco's with multiple permnos (eg berkshire)
# no filtering

# SLOW CODE (30 mins)
res <- dbSendQuery(wrds, "select DATE, PERMNO, PERMCO, CFACPR,
                          CFACSHR, SHROUT, PRC, RET, RETX, VOL
                          from CRSP.MSF")
#                   where PRC is not null")
crsp.msf <- dbFetch(res, n = -1)
save(crsp.msf, file = "crsp.msf.RData")

res <- dbSendQuery(wrds, "select DATE, PERMNO, SHRCD, EXCHCD
                   from CRSP.MSE")
#                   where SHRCD is not null")
crsp.mse <- dbFetch(res, n = -1)
save(crsp.mse, file = "crsp.mse.RData")

res <- dbSendQuery(wrds, "select DLSTDT, PERMNO, dlret
                   from crspq.msedelist")
#                   where dlret is not null")
crsp.msedelist <- dbFetch(res, n = -1)
save(crsp.msedelist, file = "crsp.msedelist.RData")

# clean and marge data
crsp.msf <- crsp.msf %>%
  dplyr::filter(!is.na(prc)) %>%
  mutate(Date = as.yearmon(as.Date(date))) %>%
  select(-date)
crsp.mse <- crsp.mse %>%
  dplyr::filter(!is.na(shrcd)) %>%
  mutate(Date = as.yearmon(as.Date(date))) %>%
  select(-date)
crsp.msedelist <- crsp.msedelist %>%
  dplyr::filter(!is.na(dlret)) %>%
  mutate(Date = as.yearmon(as.Date(dlstdt))) %>%
  select(-dlstdt)
data.crsp.m <- crsp.msf %>%
  merge(crsp.mse, by=c("Date", "permno"), all=TRUE, allow.cartesian=TRUE) %>%
  merge(crsp.msedelist, by=c("Date", "permno"), all=TRUE, allow.cartesian=TRUE) %>%
  rename(PERMNO=permno) %>%
  mutate_at(vars(PERMNO, permco, shrcd, exchcd), funs(as.factor)) %>%
  # create retadj by merging ret and dlret
  mutate(retadj=ifelse(!is.na(ret), ret, ifelse(!is.na(dlret), dlret, NA))) %>%
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%
  # fill in NA's with latest available (must sort by Date and group by PERMNO)
  mutate_at(vars(shrcd, exchcd), funs(na.locf(., na.rm=FALSE)))

data.crsp.m <- data.crsp.m %>%
  mutate(meq = shrout * abs(prc)) %>% # me for each permno
  group_by(Date, permco) %>%
  mutate(ME = sum(meq)) %>% # to calc market cap, merge permnos with same permnco
  arrange(Date, permco, desc(meq)) %>%
  group_by(Date, permco) %>%
  slice(1) %>% # keep only permno with largest meq
  ungroup

save(data.crsp.m, file = "data.crsp.m.RData")
rm(crsp.mse, crsp.msf, crsp.msedelist)

################################################################################
### CRSP CLEANING ###
# filters EXCHCD (NYSE, NASDAQ, AMEX) and SHRCD (10,11)

Fill_TS_NAs <- function(main) {
  # takes datat frame with Date and PERMNO as columns and fills in NAs
  # where there are gaps

  core <- select(main, Date, PERMNO)
  # find first and last dates of each PERMNO
  date.bookends <- core %>%
    group_by(PERMNO) %>%
    summarize(first = first(Date), last = last(Date))

  # generate all dates for all PERMNOs then trim those outside of each
  # PERMNO's first and last dates
  output <- core %>%
    # create 3rd column so spread can be applied
    mutate(temp = 1) %>%
    spread(., PERMNO, temp) %>%
    gather(., PERMNO, temp, -Date) %>%
    merge(date.bookends, by="PERMNO", all.x=TRUE) %>%
    group_by(PERMNO) %>%
    dplyr::filter(Date>=first & Date<=last) %>%
    select(Date, PERMNO) %>%
    merge(., main, by=c("Date", "PERMNO"), all.x=TRUE)

  return(output)
}

# SLOW CODE (5 mins)
load("data.crsp.m.RData")
data.crsp.cln <- data.crsp.m %>%
  select(Date, PERMNO, shrcd, exchcd, cfacpr, cfacshr, shrout, prc, vol, retx, retadj, ME) %>%
  # convert from thousands to millions (consistent with compustat values)
  mutate(ME = ME/1000) %>%
  dplyr::filter((shrcd == 10 | shrcd == 11) & (exchcd == 1 | exchcd == 2 | exchcd == 3)) %>%
  # fill in gap dates within each PERMNO with NAs to uses lead/lag
  # (lead to NAs for SHRCD and EXCHCD); fn in AnoDecomp_Support
  Fill_TS_NAs %>%
  mutate(PERMNO = as.factor(PERMNO)) %>%
  group_by(PERMNO) %>%
  # calc portweight as ME at beginning of period
  dplyr::mutate(port.weight = as.numeric(ifelse(!is.na(lag(ME)), lag(ME), ME/(1+retx))),
                # remove portweights calc for date gaps
                port.weight = ifelse(is.na(retadj) & is.na(prc), NA, port.weight)) %>%
  ungroup %>%
  rename(retadj.1mn = retadj) %>%
  arrange(Date, PERMNO) %>%
  # hasn't been issue but just in case
  distinct(Date, PERMNO, .keep_all = TRUE)

save(data.crsp.cln, file = "data.crsp.cln.RData")
rm(data.crsp.m)

################################################################################
### MERGE COMPUSTAT AND CRSP ###
# Merges CRSP and Compustat data fundamentals by PERMNO and DATE
#   (annual-June-end portfolio formation)
# Keep all CRSP info (drop Compustat if can't find CRSP)
# Match Compustat and Davis data based on FF methodology
#   (to following year June when data is first known at month end)

load("data.crsp.cln.RData")
load("data.comp.a.RData")

na_locf_until = function(x, n) {
  # in time series data, fill in na's untill indicated n
  l <- cumsum(! is.na(x))
  c(NA, x[! is.na(x)])[replace(l, ave(l, l, FUN=seq_along) > (n+1), 0) + 1]
}

# SLOW CODE (5 mins)
data.both.m <- data.comp.a %>%
  # map to next year June period when data is known (must occur in previous year)
  mutate(Date = datadate + (18-month(yearmon(datadate)))/12) %>%
  # keep all CRSP records (Compustat only goes back to 1950)
  merge(data.crsp.cln, ., by=c("Date", "PERMNO"), all.x=TRUE, allow.cartesian=TRUE) %>%
  # merge(data.Davis.bkeq, by=c("Date", "PERMNO"), all.x=TRUE, allow.cartesian=TRUE) %>%
  arrange(PERMNO, Date, desc(datadate)) %>%
  # drop older datadates (must sort by desc(datadate))
  distinct(PERMNO, Date, .keep_all = TRUE) %>%
  group_by(PERMNO) %>%
  # fill in Compustat and Davis data NA's with latest available for subsequent
  #   year (must sort by Date and group by PERMNO)
  # filling max of 11 previous months means gaps may appear when fiscal year
  #   end changes (very strict)
  # mutate_at(vars(datadate:Davis.bkeq), funs(na_locf_until(., 11))) %>%
  ungroup %>%
  mutate(datadate = yearmon(datadate)) %>%
  arrange(Date, PERMNO)

save(data.both.m, file = "data.both.m.RData")
# company info has no Date gaps (filled with NA's)
# all data publicly available by end of Date period
# (Compustat first data is June-1950 matched to CRSP Jun-51))
# includes all CRSP (but only Compustat/Davis data that matches CRSP)
# CRSP first month price data Dec-25, return data Jan-26
# CRSP last month data Dec-17
# (Compustat 2017 data available but discarded bc/ must be mapped to CRSP 2018 data)
# 180619 3.507 MM obs (Old: 170226 3.463 MM obs)
rm(data.comp.a, data.crsp.cln)


################################################################################
### Add FF Variables ###

# SLOW CODE (10 mins)
load("data.both.m.RData")
data.both.FF.m <- data.both.m %>%
  group_by(PERMNO) %>%
  # change in monthly share count (adjusted for splits)
  mutate(d.shares = (shrout*cfacshr)/(lag(shrout)*lag(cfacshr))-1,
         ret.12t2 = (lag(retadj.1mn,1)+1)*(lag(retadj.1mn,2)+1)*(lag(retadj.1mn,3)+1)*(lag(retadj.1mn,4)+1)*
           (lag(retadj.1mn,5)+1)*(lag(retadj.1mn,6)+1)*(lag(retadj.1mn,7)+1)*(lag(retadj.1mn,8)+1)*
           # to calc momentum spread
           (lag(retadj.1mn,9)+1)*(lag(retadj.1mn,10)+1)*(lag(retadj.1mn,11)+1)-1,
         # previous Dec ME
         ME.Dec = as.numeric(ifelse(month(Date)==6 & lag(ME,6)>0, lag(ME,6), NA)),
         ME.Jun = as.numeric(ifelse(month(Date)==6, ME, NA)),
         BM.FF = as.numeric(ifelse(month(Date)==6 & ME.Dec>0, BE/ME.Dec, NA)),
         OpIB = as.numeric(ifelse(month(Date)==6 & BE>0, OpProf/BE, NA)),
         GrIA = as.numeric(ifelse(month(Date)==6 & at>0, GrProf/at, NA)),
         CFP.FF = as.numeric(ifelse(month(Date)==6 & ME.Dec>0, Cflow/ME.Dec, NA)),
         # monthly updated version for spread calc
         BM.m = BE/ME,
         # monthly updated version for spread calc
         CFP.m = Cflow/ME,
         # monthly data so only lag by 1 mn
         lag.ME.Jun = lag(ME.Jun),
         lag.BM.FF = lag(BM.FF),
         lag.ret.12t2 = lag(ret.12t2),
         lag.OpIB = lag(OpIB),
         lag.AstChg = lag(AstChg))

data.both.FF.m %<>%
  # code Inf values as NAs
  mutate_at(vars(d.shares:lag.AstChg), funs(ifelse(!is.infinite(.), ., NA))) %>%
  select(Date, datadate, PERMNO, exchcd, prc, vol, retadj.1mn, d.shares, ME, port.weight,
         ret.12t2, at:AstChg, ME.Jun:lag.AstChg) %>%
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%
  mutate_at(vars(ME.Jun:CFP.FF, lag.ME.Jun:lag.AstChg), funs(na_locf_until(., 11))) %>%
  ungroup %>%
  # necessary to avoid NAs for weighted ret calc
  mutate(port.weight = ifelse(is.na(port.weight), 0, port.weight))

save(data.both.FF.m, file = "data.both.FF.m.RData")
rm(data.both.m)
