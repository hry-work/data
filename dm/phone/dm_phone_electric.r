# 本地使用
source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')
# 调度使用
# source('/root/data/env_centos.r' , encoding = 'utf8')
# 电费(当前只关注大额电欠费)
# 大额电欠费逻辑：截至上月末的应收，在截止上月末时累计欠费>=10000的，在本月的清收情况

author <- c('huruiyi')
table <- 'dm_phone_electric'

for (day in days) {
  
  day <- as_date(day)
  
  # ---------- 电费相关日期
  ys_end <- month_start - days(1)
  
  
  # ---------- 从sql server取电费相关表
  # 应收
  print(paste0('电费应收开始：' , now()))
  chargebills <- sqlQuery(con_sqls , glue("select pk_project , project_name , 
                                          pk_house , house_code , house_name , 
                                          pk_chargebills , cost_datestart , accrued_amount
                                          from mid_eve_fee_utilities_chargebills
                                          where cost_datestart < '{month_start}'
                                          and fee_type = '电费'")) %>% 
    filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
    mutate(cost_datestart = as_date(cost_datestart) ,
           pk_chargebills = trimws(pk_chargebills))
  print(paste0('电费应收结束：' , now()))
  
  # 实收
  print(paste0('电费实收开始：' , now()))
  gathering <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                        cost_datestart , real_amount , bill_date
                                        from mid_eve_fee_utilities_gathering
                                        where cost_datestart < '{month_start}'
                                        and fee_type = '电费'")) %>% 
    filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
    mutate(cost_datestart = as_date(cost_datestart) ,
           bill_date = as_date(bill_date) ,
           pk_chargebills = trimws(pk_chargebills))
  print(paste0('电费实收结束：' , now()))
  
  # 减免
  print(paste0('电费减免开始：' , now()))
  relief <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                     cost_datestart , adjust_amount , enableddate
                                     from mid_eve_fee_utilities_relief
                                     where cost_datestart < '{month_start}'
                                     and fee_type = '电费'")) %>% 
    filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
    mutate(cost_datestart = as_date(cost_datestart) ,
           enableddate = as_date(enableddate) ,
           pk_chargebills = trimws(pk_chargebills))
  print(paste0('电费减免结束：' , now()))
  
  # 冲抵
  print(paste0('电费冲抵开始：' , now()))
  match <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                    cost_datestart , match_amount , bill_date
                                    from mid_eve_fee_utilities_match
                                    where cost_datestart < '{month_start}'
                                    and fee_type = '电费'")) %>% 
    filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
    mutate(cost_datestart = as_date(cost_datestart) ,
           bill_date = as_date(bill_date) ,
           pk_chargebills = trimws(pk_chargebills))
  print(paste0('电费冲抵结束：' , now()))
  
  
  # ---------- 大额电欠费
  # 大额电欠费逻辑：截至上月末的应收，在截止上月末时累计欠费>=10000的，在本月的清收情况
  print(now())
  large_owe <- chargebills %>% 
    left_join(gathering %>% 
                filter(bill_date <= month_end) %>% 
                group_by(pk_chargebills) %>% 
                summarise(lm_real_amount = sum(real_amount[bill_date < month_start] , na.rm = T) ,
                          now_real_amount = sum(real_amount , na.rm = T))) %>% 
    left_join(relief %>% 
                filter(enableddate <= month_end) %>% 
                group_by(pk_chargebills) %>% 
                summarise(lm_adjust_amount = sum(adjust_amount[enableddate < month_start] , na.rm = T) ,
                          now_adjust_amount = sum(adjust_amount , na.rm = T))) %>% 
    left_join(match %>% 
                filter(bill_date <= month_end) %>% 
                group_by(pk_chargebills) %>% 
                summarise(lm_match_amount = sum(match_amount[bill_date < month_start] , na.rm = T) ,
                          now_match_amount = sum(match_amount , na.rm = T))) %>% 
    mutate(accrued_amount = round(replace_na(accrued_amount , 0) , 2) ,
           lm_real_amount = round(replace_na(lm_real_amount , 0),2) ,
           now_real_amount = round(replace_na(now_real_amount , 0),2) ,
           lm_adjust_amount = round(replace_na(lm_adjust_amount , 0),2) ,
           now_adjust_amount = round(replace_na(now_adjust_amount , 0),2) ,
           lm_match_amount = round(replace_na(lm_match_amount , 0),2) ,
           now_match_amount = round(replace_na(now_match_amount , 0),2) ,
           lm_owe_amount = round(accrued_amount - lm_real_amount - lm_adjust_amount - lm_match_amount,2) ,
           now_owe_amount = round(accrued_amount - now_real_amount - now_adjust_amount - now_match_amount,2)) 
    
    
    cs <- large_owe %>%
      filter(accrued_amount <= 0 , (now_real_amount >0 | now_adjust_amount > 0 |now_match_amount > 0))
    
    group_by(project_name , pk_house) %>% 
    
    group_by(project_name) %>% 
    summarise(accrued_amount = sum(accrued_amount , na.rm = T) ,
              takeover_amount = sum(get_amount , na.rm = T))
  print(now())
  
  
  # ---------- 判断日期设置重复(截至年、半年、季度、月末的数据)
  # 合并
  water_data <- water %>% 
    mutate(day = day ,
           ys_start = ys_start ,
           ys_end = ys_end ,
           get_end = month_end ,
           pd_type = 'M' ,
           pd_type_value = month_value ,
           is_complete = if_else(day == month_end , 1 , 0))
  
  # 替换空值
  water_data[is.na(water_data)] <- 0
  
  # 设置重复
  water_data <- water_data %>% 
    rbind(water_data %>% 
            mutate(pd_type = 'Q' ,
                   pd_type_value = quarter_value ,
                   is_complete = if_else(day == quarter_end , 1 , 0))) %>% 
    rbind(water_data %>% 
            mutate(pd_type = 'HY' ,
                   pd_type_value = halfyear_value ,
                   is_complete = if_else(day == halfyear_end , 1 , 0))) %>% 
    rbind(water_data %>% 
            mutate(pd_type = 'Y' ,
                   pd_type_value = year_value ,
                   is_complete = if_else(day == year_end , 1 , 0))) %>% 
    mutate(d_t = now()) 
  
  
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
  
}

