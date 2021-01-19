# 日期
today <- as_date(today())
day <- as_date(today - 1)

DAYS <- day

# 常用日期
# 年
year_start <- as_date(paste0(year(day) , '-01-01'))
year_end <- as_date(paste0(year(day) , '-12-31'))
lyear_start <- year_start - years(1)
lyear_end <- year_end - years(1)
# 半年


print('source date success')