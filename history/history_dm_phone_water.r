# 本地使用
# source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')
# 调度使用
source('/root/data/env_centos.r' , encoding = 'utf8')
# 水费逻辑：上月末所属的年初至上月末的应收，截至本月末的收缴情况(只按月来看)

author <- c('huruiyi')
table <- 'dm_phone_water'


# ---------- 从sql server取水费相关表
# 应收
print(paste0('水费应收开始：' , now()))
chargebills <- sqlQuery(con_sqls , glue("select pk_project , project_name , pk_house , 
                                          house_code , house_name , pk_chargebills , 
                                          cost_datestart , accrued_amount , projectname
                                          from mid_eve_fee_utilities_chargebills
                                          where cost_datestart < '{month_start}'
                                          and fee_type = '水费'")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         pk_chargebills = trimws(pk_chargebills))
print(paste0('水费应收结束：' , now()))

# 实收
print(paste0('水费实收开始：' , now()))
gathering <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                        cost_datestart , real_amount , bill_date
                                        from mid_eve_fee_utilities_gathering
                                        where cost_datestart < '{month_start}'
                                        and fee_type = '水费'")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         bill_date = as_date(bill_date) ,
         pk_chargebills = trimws(pk_chargebills))
print(paste0('水费实收结束：' , now()))

# 减免
print(paste0('水费减免开始：' , now()))
relief <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                     cost_datestart , adjust_amount , enableddate
                                     from mid_eve_fee_utilities_relief
                                     where cost_datestart < '{month_start}'
                                     and fee_type = '水费'")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         enableddate = as_date(enableddate) ,
         pk_chargebills = trimws(pk_chargebills))
print(paste0('水费减免结束：' , now()))

# 冲抵
print(paste0('水费冲抵开始：' , now()))
match <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                    cost_datestart , match_amount , bill_date
                                    from mid_eve_fee_utilities_match
                                    where cost_datestart < '{month_start}'
                                    and fee_type = '水费'")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         bill_date = as_date(bill_date) ,
         pk_chargebills = trimws(pk_chargebills))
print(paste0('水费冲抵结束：' , now()))


# ---------- 水费日期计算
water_date <- chargebills %>% 
  distinct(cost_datestart) %>% 
  mutate(cost_datestart = as_date(cost_datestart)) 

# 水费收费截止月末
date <- sqlQuery(con_sqls , "select day , month_end from mid_map_date") %>% 
  mutate(day = as_date(day) ,
         month_end = as_date(month_end)) %>% 
  filter(day >= min(water_date$cost_datestart) , 
         day < month_start) %>% 
  distinct(month_end)


for (day in date$month_end) {
  
  day <- as_date(day)
  # day <- as_date('2020-10-31')
  print(day)
  
  # 水费逻辑：上月末所属的年初至上月末的应收，截至本月末的收缴情况
  
  # ---------- 水费相关日期
  se_m <- get_day_start_end(day , 'M')
  month_start <- se_m$start
  month_end <- se_m$end
  month_value <- get_pd_type_value(day , 'M')
  print(paste0(month_end , '   ' , month_value))
  
  ys_end <- month_start - days(1)
  ys_start <- get_day_start_end(ys_end , 'Y')$start

  
  # ---------- 水费金额
  print(now())
  water <- chargebills %>% 
    filter(cost_datestart >= ys_start ,
           cost_datestart <= ys_end) %>% 
    left_join(gathering %>% 
                filter(cost_datestart >= ys_start ,
                       cost_datestart <= ys_end ,
                       bill_date <= month_end) %>% 
                group_by(pk_chargebills) %>% 
                summarise(real_amount = sum(real_amount , na.rm = T),
                          real_amount_tm = sum(real_amount[bill_date >= month_start] , na.rm = T))) %>% 
    left_join(relief %>% 
                filter(cost_datestart >= ys_start ,
                       cost_datestart <= ys_end ,
                       enableddate <= month_end) %>% 
                group_by(pk_chargebills) %>% 
                summarise(adjust_amount = sum(adjust_amount , na.rm = T),
                          adjust_amount_tm = sum(adjust_amount[enableddate >= month_start] , na.rm = T))) %>% 
    left_join(match %>% 
                filter(cost_datestart >= ys_start ,
                       cost_datestart <= ys_end ,
                       bill_date <= month_end) %>% 
                group_by(pk_chargebills) %>% 
                summarise(match_amount = sum(match_amount , na.rm = T),
                          match_amount_tm = sum(match_amount[bill_date >= month_start] , na.rm = T))) %>% 
    mutate(accrued = round(replace_na(accrued_amount , 0),2) ,
           real_amount = round(replace_na(real_amount , 0),2) ,
           adjust_amount = round(replace_na(adjust_amount , 0),2) ,
           match_amount = round(replace_na(match_amount , 0),2) ,
           get_amount = round(real_amount + adjust_amount + match_amount,2) ,
           real_amount_tm = round(replace_na(real_amount_tm , 0),2) ,
           adjust_amount_tm = round(replace_na(adjust_amount_tm , 0),2) ,
           match_amount_tm = round(replace_na(match_amount_tm , 0),2) ,
           get_amount_tm = round(real_amount_tm + adjust_amount_tm + match_amount_tm,2) , 
           project_type = case_when(grepl('住宅' , projectname) ~ '住宅' ,
                                    grepl('商业' , projectname) ~ '商业' ,
                                    TRUE ~ '其他')) %>% 
    group_by(project_name) %>% 
    summarise(accrued_amount = sum(accrued , na.rm = T) ,
              takeover_amount = sum(get_amount , na.rm = T) ,
              takeover_amount_tm = sum(get_amount_tm , na.rm = T) ,
              accrued_amount_house = sum(accrued[project_type == '住宅'] , na.rm = T) ,
              takeover_amount_house = sum(get_amount[project_type == '住宅'] , na.rm = T) ,
              takeover_amount_house_tm = sum(get_amount_tm[project_type == '住宅'] , na.rm = T) ,
              accrued_amount_bussiness = sum(accrued[project_type == '商业'] , na.rm = T) ,
              takeover_amount_bussiness = sum(get_amount[project_type == '商业'] , na.rm = T) ,
              takeover_amount_bussiness_tm = sum(get_amount_tm[project_type == '商业'] , na.rm = T) ,
              accrued_amount_other = sum(accrued[project_type == '其他'] , na.rm = T) ,
              takeover_amount_other = sum(get_amount[project_type == '其他'] , na.rm = T) ,
              takeover_amount_other_tm = sum(get_amount_tm[project_type == '其他'] , na.rm = T) ,) 
  print(now())
  
  
  # ---------- 水只有按月的看法
  # 合并
  water_data <- water %>% 
    mutate(day = day ,
           ys_start = ys_start ,
           ys_end = ys_end ,
           get_end = month_end ,
           pd_type = 'M' ,
           pd_type_value = month_value ,
           is_complete = if_else(day == month_end , 1 , 0),
           d_t = now())
  
  # 替换空值
  water_data[is.na(water_data)] <- 0
  
  
  # ---------- 入库
  # 连接mysql dm层
  conn <- dbConnect(con_dm)
  
  # 删除除新跑数据外，非月末、季末、半年末、年末的数据(is_complete为0的)
  delete_uncomplete <- dbGetQuery(conn , glue("delete from {table} where is_complete = 0"))
  delete_old <- dbGetQuery(conn , glue("delete from {table} where day = '{day}'"))
  print('delete success')
  
  # 今日新跑数据写入(windows会报错，带中文的字符串需改为gbk)
  dbWriteTable(conn , table , water_data , append = T , row.names = F)
  print('write success')
  
  # 断开连接
  dbDisconnect(conn)
  
}

