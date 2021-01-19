# RScript参数
# RScript path/*.R --start '2018-01-01' --rpt_type 'M'
# RScript path/*.R -s '2018-01-01' -rt 'M'
spec = matrix(c(
  'start',       's',   2, "character",
  'end'  ,       'e',   2, "character",
  'pd_type',     'pd',  2, "character", # Y\HY\Q\M\W\D
  'days' ,       'd',   2, "character",
  'noop_start',  'n',   2, "character" #
), byrow=TRUE, ncol=4)
opt = getopt(spec)
print(opt)

# 接收Azkaban传入参数，进行ETL调度
# 传入的日期都会被执行，etl_end会处理为传入的最后1天加1
# >=etl_start
# < etl_end
# >=START <=END
is_blank <- function(x){
  if (is.null(x)){
    return(0)
  } else {
    return(nchar(x))
  }
}

if (is_blank(opt$start) == 0) {
  etl_start <- as_date(today()-1)
} else {
  etl_start <- as_date(opt$start)
}

if (is_blank(opt$end) == 0) {
  etl_end <- as_date(today())
} else {
  etl_end <- as_date(opt$end) + days(1)
}

if (is.null(opt$pd_type)) {
  pd_type <- c('Y', 'HY', 'Q', 'M', 'W', 'D')
} else {
  pd_type <- str_split(opt$pd_type, ',')[[1]]
}

if (is_blank(opt$days) == 0) {
  days <- seq(ymd(etl_start), ymd(etl_end)-1, by = 1)
} else {
  if (str_count(opt$days, '~') > 0){
    etl_start <- str_sub(opt$days, 1, 10)
    etl_end <- ymd(str_sub(opt$days, 12)) + 1
    days <- seq(ymd(etl_start), ymd(etl_end)-1, by = 1)
  } else if (str_count(opt$days, ',') > 0){
    days <- str_extract_all(opt$days, '[0-9]{4}-[0-9]{2}-[0-9]{2}')[[1]]
  } else {
    etl_start <- opt$days
    etl_end <- as.character(ymd(etl_start) + 1)
    days <- seq(ymd(etl_start), ymd(etl_end)-1, by = 1)
  }
}

if (is_blank(opt$noop_start) == 0) {
  NOOP_START <- ''
} else {
  NOOP_START <- opt$noop_start
}

