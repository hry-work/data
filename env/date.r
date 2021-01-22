# 日期
today <- as_date(today())
day <- as_date(today - 1)


# 常用日期
# 年
year_start <- as_date(paste0(year(day) , '-01-01'))
year_end <- as_date(paste0(year(day) , '-12-31'))
lyear_start <- year_start - years(1)
lyear_end <- year_end - years(1)
year_value <- get_pd_type_value(day , 'Y')
# 半年
halfyear <- get_day_start_end(day , 'HY')
halfyear_start <- halfyear$start
halfyear_end <- halfyear$end
halfyear_value <- get_pd_type_value(day , 'HY')
# lyear_start <- halfyear_start - years(1)
# lyear_end <- halfyear_end - years(1)
# 季度
quarter <- get_day_start_end(day , 'Q')
quarter_start <- quarter$start
quarter_end <- quarter$end
quarter_value <- get_pd_type_value(day , 'Q')
# 月
month <- get_day_start_end(day , 'M')
month_start <- month$start
month_end <- month$end
month_value <- get_pd_type_value(day , 'M')


print('source date success')