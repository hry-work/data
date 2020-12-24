source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_map_date2'      # 日期表

day <- as.data.frame(seq.Date(as_date('1990-01-01') , as_date('2100-12-31') , by = 'day')) 
colnames(day) <- c('day')

datetype <- as.data.frame(c('year' , 'halfyear' , 'quarter' , 'month'))
colnames(datetype) <- c('datetype')


day_data <- day %>% 
  mutate(id = 1) %>% 
  join(datetype %>% mutate(id = 1) , by = 'id') %>% 
  mutate(year = paste0(year(day) , '年') ,#day所属的年份
         year_start = as_date(paste0(year(day) , '-01-01')) ,#day所属年份的开始 适用物业费、车位费应收开始
         year_end = as_date(paste0(year(day) , '-12-31')) ,#day所属年份的结束
         halfyear = if_else(month(day) <= 6 , paste0(year(day) , '-上半年') , paste0(year(day) , '-下半年')) ,#day所属的半年份
         halfyear_start = if_else(month(day) <= 6 , as_date(paste0(year , '-01-01')) , as_date(paste0(year , '-07-01'))) ,#day所属的半年份开始
         halfyear_end = if_else(month(day) <= 6 , as_date(paste0(year , '-06-30')) , as_date(paste0(year , '-12-31'))) ,#day所属的半年份结束
         quarter_eng = quarters(day) ,#day所属的季度
         quarter = paste0(year(day) , '-' , substr(quarter_eng , 2 , 2) , '季度') ,#day所属的季度
         quarter_start = case_when(quarter_eng == 'Q1' ~ as_date(paste0(year(day) , '-01-01')) ,
                                   quarter_eng == 'Q2' ~ as_date(paste0(year(day) , '-04-01')) ,
                                   quarter_eng == 'Q3' ~ as_date(paste0(year(day) , '-07-01')) ,
                                   quarter_eng == 'Q4' ~ as_date(paste0(year(day) , '-10-01'))) ,#day所属的季度开始
         quarter_end = case_when(quarter_eng == 'Q1' ~ as_date(paste0(year(day) , '-03-31')) ,
                                 quarter_eng == 'Q2' ~ as_date(paste0(year(day) , '-06-30')) ,
                                 quarter_eng == 'Q3' ~ as_date(paste0(year(day) , '-09-30')) ,
                                 quarter_eng == 'Q4' ~ as_date(paste0(year(day) , '-12-31'))) ,#day所属的季度结束
         month = substr(day , 1 , 7) , #day所属的月份
         month_start = as_date(paste0(substr(day , 1 , 8) , '01')), #day所属的月份开始
         month_end = as_date(ceiling_date(day , 'month')) - days(1) , #水费、大额电欠费收款截止
         period = case_when(datetype == 'year' ~ year ,
                            datetype == 'halfyear' ~ halfyear ,
                            datetype == 'quarter' ~ quarter ,
                            datetype == 'month' ~ month) ,#day所属的周期
         period_start = case_when(datetype == 'year' ~ year_start ,
                                  datetype == 'halfyear' ~ halfyear_start ,
                                  datetype == 'quarter' ~ quarter_start ,
                                  datetype == 'month' ~ as_date(paste0(month , '-01'))) ,#day所属的周期开始
         period_end = case_when(datetype == 'year' ~ year_end ,
                                datetype == 'halfyear' ~ halfyear_end ,
                                datetype == 'quarter' ~ quarter_end ,
                                datetype == 'month' ~ as_date(ceiling_date(day , 'month')) - days(1)) ,#day所属的周期结束
         ys_start_utilities = case_when(datetype == 'month' & month(day) == 1 ~ as_date(paste0(year(day) - 1 , '-01-01')) , 
                                        datetype == 'month' & month(day) != 1 ~ as_date(paste0(year(day) , '-01-01')) ,
                                        TRUE ~ as_date(NA_character_)) , #水电应收期间开始，水电只有周期为月时有数据
         ys_end_utilities = if_else(datetype == 'month' , as_date(floor_date(month_end , 'month')) - days(1) , as_date(NA_character_)) ,#水电应收期间结束，只有周期为月时有数据
         last_year_start = year_start - years(1) ,
         last_year_end = year_end - years(1) ,
         period_tb_start = case_when(datetype == 'year' ~ year_start - years(1) ,
                                     datetype == 'halfyear' ~ halfyear_start - years(1) ,
                                     datetype == 'quarter' ~ quarter_start - years(1) ,
                                     datetype == 'month' ~ month_start - years(1)) , #day所属的周期同比开始
         period_tb_end = case_when(datetype == 'year' ~ year_end - years(1) ,
                                   datetype == 'halfyear' ~ halfyear_end - years(1) ,
                                   datetype == 'quarter' ~ quarter_end - years(1) ,
                                   datetype == 'month' ~ as_date(ceiling_date(period_tb_start , 'month')) - days(1)) , #day所属的周期同比结束
         period_tb = case_when(datetype == 'year' ~ paste0(year(period_tb_start), '年') ,
                               datetype == 'halfyear' & month(period_tb_start) <= 6 ~ paste0(year(period_tb_start) , '-上半年') ,
                               datetype == 'halfyear' & month(period_tb_start) > 6 ~ paste0(year(period_tb_start) , '-下半年') ,
                               datetype == 'quarter' & month(period_tb_start) <= 3 ~ paste0(year(period_tb_start) , '-1季度') ,
                               datetype == 'quarter' & month(period_tb_start) <= 6 ~ paste0(year(period_tb_start) , '-2季度') ,
                               datetype == 'quarter' & month(period_tb_start) <= 9 ~ paste0(year(period_tb_start) , '-3季度') ,
                               datetype == 'quarter' & month(period_tb_start) <= 12 ~ paste0(year(period_tb_start) , '-4季度') ,
                               datetype == 'month' ~ substr(period_tb_start , 1 , 7)) , #day所属的周期同比
         period_hb_start = case_when(datetype == 'year' ~ period_tb_start ,
                                     datetype == 'halfyear' ~ halfyear_start - months(6) ,
                                     datetype == 'quarter' ~ quarter_start - months(3) ,
                                     datetype == 'month' ~ month_start - months(1)) , #day所属的周期环比开始
         period_hb_end = period_start - days(1) , #day所属的周期环比结束
         period_hb = case_when(datetype == 'year' ~ period_tb ,
                               datetype == 'halfyear' & month(period_hb_start) <= 6 ~ paste0(year(period_hb_start) , '-上半年') ,
                               datetype == 'halfyear' & month(period_hb_start) > 6 ~ paste0(year(period_hb_start) , '-下半年') ,
                               datetype == 'quarter' & month(period_hb_start) <= 3 ~ paste0(year(period_hb_start) , '-1季度') ,
                               datetype == 'quarter' & month(period_hb_start) <= 6 ~ paste0(year(period_hb_start) , '-2季度') ,
                               datetype == 'quarter' & month(period_hb_start) <= 9 ~ paste0(year(period_hb_start) , '-3季度') ,
                               datetype == 'quarter' & month(period_hb_start) <= 12 ~ paste0(year(period_hb_start) , '-4季度') ,
                               datetype == 'month' ~ substr(period_hb_start , 1 , 7)) , #day所属的周期环比
         ys_start_utilities_tb = ys_start_utilities - years(1) , #水电同比应收期间开始，只有周期为月时有数据
         ys_end_utilities_tb = ceiling_date(as_date(if_else(datetype == 'month' , 
                                                            paste(year(ys_start_utilities_tb) , substr(ys_end_utilities , 6 , 7) , '01' , sep = '-') , 
                                                            NA_character_)) , 'month') - days(1) , #水电同比应收期间结束，只有周期为月时有数据
         get_end_utilities_tb = ceiling_date(ys_end_utilities_tb , 'month') + months(1) -days(1) ,
         next_year_start = year_start + years(1) ,
         next_year_end = year_end + years(1) ,
         next_period_start = case_when(datetype == 'year' ~ next_year_start ,
                                       datetype == 'halfyear' ~ period_start + months(6) ,
                                       datetype == 'quarter'  ~ period_start + months(3) ,
                                       datetype == 'month' ~ period_start + months(1)) , #day所属周期的下个周期开始
         next_period_end = case_when(datetype == 'year' ~ next_year_end ,
                                     datetype == 'halfyear' ~ next_period_start + months(6) - days(1) ,
                                     datetype == 'quarter'  ~ next_period_start + months(3) - days(1) ,
                                     datetype == 'month' ~ next_period_start + months(1) - days(1)) , #day所属周期的下个周期结束
         next_period = case_when(datetype == 'year' ~ paste0(year(next_period_start), '年') ,
                                 datetype == 'halfyear' & month(next_period_start) <= 6 ~ paste0(year(next_period_start) , '-上半年') ,
                                 datetype == 'halfyear' & month(next_period_start) > 6 ~ paste0(year(next_period_start) , '-下半年') ,
                                 datetype == 'quarter' & month(next_period_start) <= 3 ~ paste0(year(next_period_start) , '-1季度') ,
                                 datetype == 'quarter' & month(next_period_start) <= 6 ~ paste0(year(next_period_start) , '-2季度') ,
                                 datetype == 'quarter' & month(next_period_start) <= 9 ~ paste0(year(next_period_start) , '-3季度') ,
                                 datetype == 'quarter' & month(next_period_start) <= 12 ~ paste0(year(next_period_start) , '-4季度') ,
                                 datetype == 'month' ~ substr(next_period_start , 1 , 7)) ,
         d_t = now()) %>% 
  select(day , datetype , year , year_start , year_end , ys_start_utilities , ys_end_utilities , month_end , 
         halfyear , quarter_eng , quarter , month , period , period_start , period_end ,
         last_year_start , last_year_end , period_tb , period_tb_start , period_tb_end , 
         period_hb , period_hb_start , period_hb_end ,ys_start_utilities_tb , ys_end_utilities_tb , get_end_utilities_tb ,
         next_period , next_period_start , next_period_end , next_year_start , next_year_end , d_t) %>% 
  rename(get_end_utilities = month_end)


# sqlserver入库
sqlClear(con_sql, table)
sqlSave(con_sql , day_data , tablename = table ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL map day_data2 success: ' , now()))




