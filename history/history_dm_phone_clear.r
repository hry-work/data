# 本地使用
# source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')
# 调度使用
source('/root/data/env_centos.r' , encoding = 'utf8')

author <- c('huruiyi')
table <- 'dm_phone_property_clear'
print('跑历史数据')

# ---------- 从sql server取物业费相关表
# 应收
print(paste0('应收开始：' , now()))
chargebills <- sqlQuery(con_sqls , glue("select belong , project1 , project2 , 
                                          project3 , project4 , pk_project , project_name , 
                                          pk_house , house_code , house_name , wy_cycle , 
                                          pk_chargebills , cost_datestart , accrued_amount , projectname
                                          from mid_eve_fee_property_chargebills")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         pk_chargebills = trimws(pk_chargebills))
print(paste0('应收结束：' , now()))

# 实收
print(paste0('实收开始：' , now()))
gathering <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                        cost_datestart , real_amount , bill_date
                                        from mid_eve_fee_property_gathering")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         bill_date = as_date(bill_date) ,
         pk_chargebills = trimws(pk_chargebills))
print(paste0('实收结束：' , now()))

# 减免
print(paste0('减免开始：' , now()))
relief <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                     cost_datestart , adjust_amount , enableddate
                                     from mid_eve_fee_property_relief")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         enableddate = as_date(enableddate) ,
         pk_chargebills = trimws(pk_chargebills))
print(paste0('减免结束：' , now()))

# 冲抵
print(paste0('冲抵开始：' , now()))
match <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                    cost_datestart , match_amount , bill_date
                                    from mid_eve_fee_property_match")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         bill_date = as_date(bill_date) ,
         pk_chargebills = trimws(pk_chargebills))
print(paste0('冲抵结束：' , now()))

# 欠费原因
basic_info <- sqlQuery(con_sqls , glue("select pk_house , owe_type
                                         from mid_dim_owner_basic_info "))


# ---------- 物业费日期计算
property_date <- chargebills %>% 
  distinct(cost_datestart) %>% 
  mutate(cost_datestart = as_date(cost_datestart)) %>% 
  filter(cost_datestart < year_start)

# 物业费收费截止月末
date <- sqlQuery(con_sqls , "select day , month_end from mid_map_date") %>% 
  mutate(day = as_date(day) ,
         month_end = as_date(month_end)) %>% 
  filter(day >= min(property_date$cost_datestart) , 
         day < month_start) %>% 
  distinct(month_end)


for (day in date$month_end) {
  
  day <- as_date(day)
  # day <- as_date('2006-10-31')
  print(day)
  
  se_y <- get_day_start_end(day , 'Y')
  year_start <- se_y$start
  year_end <- se_y$end
  year_value <- get_pd_type_value(day , 'Y')
  print(paste0(year_start , '-' , year_end , '   ' , year_value))
  
  se_hy <- get_day_start_end(day , 'HY')
  halfyear_end <- se_hy$end
  halfyear_value <- get_pd_type_value(day , 'HY')
  print(paste0(halfyear_end , '   ' , halfyear_value))
  
  se_q <- get_day_start_end(day , 'Q')
  quarter_end <- se_q$end
  quarter_value <- get_pd_type_value(day , 'Q')
  print(paste0(quarter_end , '   ' , quarter_value))
  
  se_m <- get_day_start_end(day , 'M')
  month_end <- se_m$end
  month_value <- get_pd_type_value(day , 'M')
  print(paste0(month_end , '   ' , month_value))
  
  
  # ---------- 清欠
  # 应收：每年1日之前的应收，在该年1日之前未收的
  # 清收：清欠应收截至该年末/半年末/季末/月末的清收的金额(跑截至到每个月月末的，特定月份的月末即为年末、半年末、季末)
  
  # 截至本年初的欠费(饱和清欠)
  print(now())
  clear_data <- chargebills %>% 
    filter(cost_datestart < year_start) %>% 
    left_join(gathering %>% 
                filter(cost_datestart < year_start , 
                       bill_date < year_start) %>% 
                group_by(pk_chargebills) %>% 
                summarise(real_amount = sum(real_amount , na.rm = T))) %>% 
    left_join(relief %>% 
                filter(cost_datestart < year_start , 
                       enableddate < year_start) %>% 
                group_by(pk_chargebills) %>% 
                summarise(adjust_amount = sum(adjust_amount , na.rm = T))) %>% 
    left_join(match %>% 
                filter(cost_datestart < year_start , 
                       bill_date < year_start) %>% 
                group_by(pk_chargebills) %>% 
                summarise(match_amount = sum(match_amount , na.rm = T))) %>% 
    mutate(real_amount = round(replace_na(real_amount , 0),2) ,
           adjust_amount = round(replace_na(adjust_amount , 0),2) ,
           match_amount = round(replace_na(match_amount , 0),2) ,
           owe_amount = round(accrued_amount - real_amount - adjust_amount - match_amount,2)) 
    # filter(owe_amount > 0) %>% 此条注掉，因历史数据部分存在错位，即上个月欠费为负，本月欠费为正，刚好充消
  
  clear <- clear_data %>% 
    filter(owe_amount > 0) %>% 
    group_by(project_name , pk_house) %>% 
    summarise(owe_amount = sum(owe_amount , na.rm = T),
              first_owe = min(cost_datestart , na.rm = T))
  
  # 到项目的欠费额
  saturation_recovery <- clear %>% 
    group_by(project_name) %>% 
    summarise(saturation_recovery = sum(owe_amount))
  
  # 今年截至日期所属月末的清欠
  recovery <- gathering %>% 
    filter(cost_datestart < year_start ,
           bill_date >= year_start , bill_date <= month_end) %>% 
    group_by(project_name) %>% 
    summarise(real_amount = sum(real_amount , na.rm = T)) %>% 
    full_join(relief %>% 
                filter(cost_datestart < year_start ,
                       enableddate >= year_start , enableddate <= month_end) %>% 
                group_by(project_name) %>% 
                summarise(adjust_amount = sum(adjust_amount , na.rm = T))) %>% 
    full_join(match %>% 
                filter(cost_datestart < year_start ,
                       bill_date >= year_start , bill_date <= month_end) %>% 
                group_by(project_name) %>% 
                summarise(match_amount = sum(match_amount , na.rm = T))) %>% 
    mutate(real_amount = replace_na(real_amount , 0) ,
           adjust_amount = replace_na(adjust_amount , 0) ,
           match_amount = replace_na(match_amount , 0) ,
           recovery = round(real_amount + adjust_amount + match_amount , 2)) %>% 
    select(project_name , recovery) 
  
  # 截至本年初欠费的房间欠费原因及欠费时长分布情况
  owe_reason <- clear %>% 
    left_join(basic_info) %>% 
    mutate(owe_type = if_else(is.na(owe_type) , '未填写欠费原因' , owe_type)) %>% 
    group_by(project_name , owe_type) %>% 
    summarise(house_cnt = n_distinct(pk_house) ,
              owe_amount = sum(owe_amount , na.rm = T)) %>% 
    mutate(key_d = 'reason') %>% 
    rename(value_d = owe_type)
  
  owe_along <-  clear %>% 
    mutate(last_period = (as.yearmon(day)-as.yearmon(first_owe))*12 ,
           owe_along = case_when(last_period <= 12 ~ '1年以内' ,
                                 last_period <= 60 ~ '5年以内' ,
                                 TRUE ~ '5年以上')) %>% 
    group_by(project_name , owe_along) %>% 
    summarise(house_cnt = n_distinct(pk_house) ,
              owe_amount = sum(owe_amount , na.rm = T)) %>% 
    mutate(key_d = 'along') %>% 
    rename(value_d = owe_along)
  
  
  # ---------- 合并物业费及清欠数据，再判断日期设置重复(截至年、半年、季度、月末的数据)
  # 合并
  recovered <- bind_rows(owe_reason , owe_along) %>% 
    mutate(day = day ,
           ys_start = year_start ,
           ys_end = year_end ,
           get_end = month_end ,
           pd_type = 'M' ,
           pd_type_value = month_value ,
           is_complete = if_else(day == month_end , 1 , 0))
  
  # 替换空值
  recovered[is.na(recovered)] <- 0
  
  # 设置重复
  recovered <- recovered %>% 
    rbind(recovered %>% 
            mutate(get_end = quarter_end ,
                   pd_type = 'Q' ,
                   pd_type_value = quarter_value ,
                   is_complete = if_else(day == quarter_end , 1 , 0))) %>% 
    rbind(recovered %>% 
            mutate(get_end = halfyear_end ,
                   pd_type = 'HY' ,
                   pd_type_value = halfyear_value ,
                   is_complete = if_else(day == halfyear_end , 1 , 0))) %>% 
    rbind(recovered %>% 
            mutate(get_end = year_end ,
                   pd_type = 'Y' ,
                   pd_type_value = paste0(year_value , '年') ,
                   is_complete = if_else(day == year_end , 1 , 0))) %>% 
    mutate(d_t = now()) 
  
  
  # ---------- 入库
  # 连接mysql dm层
  conn <- dbConnect(con_dm)
  
  # 删除除新跑数据外，非月末、季末、半年末、年末的数据(is_complete为0的)
  delete_uncomplete <- dbGetQuery(conn , glue("delete from {table} where is_complete = 0"))
  delete_old <- dbGetQuery(conn , glue("delete from {table} where day = '{day}'"))
  print('delete success')
  
  # 今日新跑数据写入(windows会报错，带中文的字符串需改为gbk。线上跑数无问题，因此可直接放线上测试)
  dbWriteTable(conn , table , recovered , append = T , row.names = F)
  print('write success')
  
  # 断开连接
  dbDisconnect(conn)
  
}

