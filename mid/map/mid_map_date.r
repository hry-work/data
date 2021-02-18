# 本地使用
# source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')
# 调度使用
source('/root/data/env_centos.r' , encoding = 'utf8')
author <- c('huruiyi')
# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_map_date'      # 日期表

day <- as.data.frame(seq.Date(as_date('1990-01-01') , as_date('2200-12-31') , by = 'day')) 

# day <- as.data.frame(as_date('1992-02-29'))
colnames(day) <- c('day')

day_data <- day %>% 
  mutate(week = weekdays(day) ,
         weekofyear = isoweek(day) ,
         week_start = floor_date(day , 'week' , week_start = getOption("lubridate.week.start", 1)) ,
         week_end = ceiling_date(day , 'week' , week_start = getOption("lubridate.week.start", 1)) - days(1) ,
         month = substr(day , 1 , 7) ,
         month_start = as_date(paste0(substr(day , 1 , 8) , '01')),
         month_end = as_date(ceiling_date(day , 'month')) - days(1) ,
         month_start_hb = month_start - months(1) , 
         month_end_hb = as_date(ceiling_date(month_start_hb , 'month')) - days(1) ,
         month_start_tb = month_start - years(1) ,
         month_end_tb = as_date(ceiling_date(month_start_tb , 'month')) - days(1) ,
         next_month_start = month_start + months(1) , 
         next_month_end = as_date(ceiling_date(next_month_start , 'month')) - days(1) ,
         hb_month = substr(month_start_hb , 1 , 7) ,
         tb_month = substr(month_start_tb , 1 , 7) ,
         next_month = substr(next_month_start , 1 , 7) ,
         quarter = quarters(day) ,
         quarter_value = paste0(year(day) , '-' , substr(quarter , 2 , 2) , '季度') ,
         quarter_start = case_when(quarter == 'Q1' ~ as_date(paste0(year(day) , '01-01')) ,
                                   quarter == 'Q2' ~ as_date(paste0(year(day) , '04-01')) ,
                                   quarter == 'Q3' ~ as_date(paste0(year(day) , '07-01')) ,
                                   quarter == 'Q4' ~ as_date(paste0(year(day) , '10-01'))) ,
         quarter_end = case_when(quarter == 'Q1' ~ as_date(paste0(year(day) , '03-31')) ,
                                 quarter == 'Q2' ~ as_date(paste0(year(day) , '06-30')) ,
                                 quarter == 'Q3' ~ as_date(paste0(year(day) , '09-30')) ,
                                 quarter == 'Q4' ~ as_date(paste0(year(day) , '12-31'))) ,
         quarter_start_hb = quarter_start - months(3) , 
         quarter_end_hb = as_date(ceiling_date(quarter_start - months(1) , 'month')) - days(1) ,
         quarter_start_tb = quarter_start - years(1) ,
         quarter_end_tb = as_date(floor_date(quarter_start_tb + months(3), 'month')) - days(1) ,
         next_quarter_start =as_date(floor_date(quarter_start + months(3) , 'month')) ,
         next_quarter_end = as_date(floor_date(next_quarter_start + months(3) , 'month')) - days(1) ,
         hb_quarter = quarters(quarter_start_hb) ,
         tb_quarter = quarters(quarter_start_tb) ,
         next_quarter = quarters(next_quarter_start) ,
         hb_quarter_value = paste0(year(quarter_start_hb) , '-' , substr(hb_quarter , 2 , 2) , '季度') ,
         tb_quarter_value = paste0(year(quarter_start_tb) , '-' , substr(tb_quarter , 2 , 2) , '季度') ,
         next_quarter_value = paste0(year(next_quarter_start) , '-' , substr(next_quarter , 2 , 2) , '季度') ,
         year = year(day) ,
         halfyear = if_else(month(day) <= 6 , paste0(year , '-上半年') , paste0(year , '-下半年')) ,
         halfyear_start = if_else(month(day) <= 6 , as_date(paste0(year , '01-01')) , as_date(paste0(year , '07-01'))) ,
         halfyear_end = if_else(month(day) <= 6 , as_date(paste0(year , '06-30')) , as_date(paste0(year , '12-31'))) ,
         halfyear_start_hb = halfyear_start - months(6) ,
         halfyear_end_hb = as_date(ceiling_date(halfyear_start - months(1) , 'month')) - days(1) ,
         halfyear_start_tb = halfyear_start - years(1) ,
         halfyear_end_tb = as_date(floor_date(halfyear_start_tb + months(6), 'month')) - days(1) ,
         hb_halfyear = if_else(month(halfyear_start_hb) <= 6 , paste0(year(halfyear_start_hb) , '-上半年') , paste0(year(halfyear_start_hb) , '-下半年')) ,
         tb_halfyear = if_else(month(halfyear_start_tb) <= 6 , paste0(year(halfyear_start_tb) , '-上半年') , paste0(year(halfyear_start_tb) , '-下半年')) ,
         next_halfyear_start = halfyear_start + months(6) ,
         next_halfyear_end = as_date(floor_date(next_halfyear_start + months(6) , 'month')) - days(1) ,
         next_halfyear = if_else(month(next_halfyear_start) <= 6 , paste0(year(next_halfyear_start) , '-上半年') , paste0(year(next_halfyear_start) , '-下半年')) ,
         next_year = year(day) + 1 ,
         year_start = as_date(paste0(year(day) , '01-01')),
         year_end = as_date(paste0(year(day) , '12-31')),
         year_start_thb = year_start - years(1) , 
         year_end_thb = as_date(ceiling_date(year_start - months(1) , 'year')) - days(1) ,
         thb_year = year(year_start_thb) ,
         next_year_start = as_date(paste0(next_year , '01-01')),
         next_year_end = as_date(paste0(next_year , '12-31')),
         d_t = now()) %>% 
  select(day , week , weekofyear , week_start , week_end , 
         month , month_start , month_end , 
         hb_month , month_start_hb , month_end_hb , 
         tb_month , month_start_tb , month_end_tb ,
         next_month , next_month_start , next_month_end , 
         quarter , quarter_value , quarter_start , quarter_end , 
         hb_quarter , hb_quarter_value , quarter_start_hb , quarter_end_hb , 
         tb_quarter , tb_quarter_value , quarter_start_tb , quarter_end_tb ,
         next_quarter , next_quarter_value , next_quarter_start , next_quarter_end ,
         halfyear , halfyear_start , halfyear_end ,
         hb_halfyear , halfyear_start_hb , halfyear_end_hb ,
         tb_halfyear , halfyear_start_tb , halfyear_end_tb ,
         next_halfyear , next_halfyear_start , next_halfyear_end ,
         year , year_start , year_end , 
         thb_year , year_start_thb , year_end_thb , 
         next_year , next_year_start , next_year_end , d_t)

  
# floor_date(日期，unit) unit比如说是年月日什么的，
# floor_date(2014-3-12,”year”)将会得到2014-01-01，就是这一日期按照年计算的最低值
# ceiling_date()正好相反，ceiling(2014-03-14,”year”)将会得到2015-01-01



# # sqlserver入库(sql server早先已入库，因此注掉不再入库)
# sqlClear(con_sqls, table)
# sqlSave(con_sqls , day_data , tablename = table ,
#         append = TRUE , rownames = FALSE , fast = FALSE)
# 
# print(paste0('ETL map day_data success: ' , now()))


# oracle暂未入库
# oracle入库，注意表名一定要大写
# newData <- project_data[,utfCol:=iconv(gbkCol,from="gbk",to="utf-8")]
# dbWriteTable(con_orc , 'MID_DIM_PROJECT_HIERARCHY' , day_data , overwrite=TRUE)
# print(paste0('ETL map day_data success: ' , now()))


# MySQL入库
conn <- dbConnect(con_mid)
# 先清空表内数据，再插入数据。因直接使用覆盖数据，会重新建表，新建表的字段类型不符合规则
delete <- dbGetQuery(conn , glue("truncate table {table};"))
dbWriteTable(conn , table , day_data , append = TRUE , row.names = FALSE)

print(paste0('MySQL ETL day_data success: ' , now()))