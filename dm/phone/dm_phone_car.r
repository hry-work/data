# 本地使用
# source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')
# 调度使用
source('/root/data/env_centos.r' , encoding = 'utf8')
# 停车费
# 当前停车费模式存在过几个月后生成之前月份的应收(收款时才生成应收)，因此设置为全量
# 本期：本年初-本月末应收的停车费，截至当前的收费情况
# 前期欠费：本年之前应收的停车费，在本年之前未收的，于本年的收缴情况
# 停车费按现金流，不按权责，因此计算税率时，按收费日期时的税率进行计算，不按生成的应收日期的税率

author <- c('huruiyi')
table <- 'dm_phone_car'

# ---------- 从sql server取停车费相关表
# 应收
print(paste0('停车费应收开始：' , now()))
chargebills <- sqlQuery(con_sqls , glue("select pk_project , project_name , pk_house , 
                                         house_code , house_name , pk_chargebills , 
                                         cost_datestart , accrued_amount , parking_type
                                         from mid_eve_fee_parking_chargebills
                                         where cost_datestart <= '{month_end}'")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         pk_chargebills = trimws(pk_chargebills))
print(paste0('停车费应收结束：' , now()))

# 实收
print(paste0('停车费实收开始：' , now()))
gathering <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                        cost_datestart , real_amount , bill_date
                                        from mid_eve_fee_parking_gathering2
                                        where cost_datestart <= '{month_end}'")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         bill_date = as_date(bill_date) ,
         pk_chargebills = trimws(pk_chargebills))
print(paste0('停车费实收结束：' , now()))

# 减免
print(paste0('停车费减免开始：' , now()))
relief <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                     cost_datestart , adjust_amount , enableddate
                                     from mid_eve_fee_parking_relief
                                     where cost_datestart <= '{month_end}'")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         enableddate = as_date(enableddate) ,
         pk_chargebills = trimws(pk_chargebills))
print(paste0('停车费减免结束：' , now()))

# 冲抵
print(paste0('停车费冲抵开始：' , now()))
match <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                    cost_datestart , match_amount , bill_date , virement_date
                                    from mid_eve_fee_parking_match
                                    where cost_datestart <= '{month_end}'")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         bill_date = as_date(bill_date) ,
         virement_date = as_date(virement_date) ,
         pk_chargebills = trimws(pk_chargebills))
print(paste0('停车费冲抵结束：' , now()))

# 税率表
# 按实收月份的税率，不按应收
car_ratio <- sqlQuery(con_sqls , glue("select project_name , tax_ratio , tax_start , tax_end
                                         from mid_dim_tax_ratio
                                         where tax_type = 'parking'"))


# ---------- 停车费日期计算
car_date <- chargebills %>% 
  distinct(cost_datestart) %>% 
  mutate(cost_datestart = as_date(cost_datestart)) %>% 
  filter(cost_datestart <= day)

# 停车费收费截止月末
date <- sqlQuery(con_sqls , "select day , month_end from mid_map_date") %>% 
  mutate(day = as_date(day) ,
         month_end = as_date(month_end)) %>% 
  filter(day >= min(car_date$cost_datestart) , 
         day < month_start) %>% 
  distinct(month_end) %>% 
  rename(run_date = month_end)

if(day %in% date$run_date){
  date <- date
} else {
  date <- date %>% 
    rbind(data.frame('run_date' = day))
}


for (day in date$run_date) {
  
  day <- as_date(day)
  print(day)
  
  # ---------- 停车费相关日期
  se_m <- get_day_start_end(day , 'M')
  month_end <- se_m$end
  month_value <- get_pd_type_value(day , 'M')
  print(paste0(month_end , '   ' , month_value))
  
  ys_start <- get_day_start_end(month_end , 'Y')$start
  
  se_y <- get_day_start_end(day , 'Y')
  year_end <- se_y$end
  year_value <- get_pd_type_value(day , 'Y')
  
  se_hy <- get_day_start_end(day , 'HY')
  halfyear_end <- se_hy$end
  halfyear_value <- get_pd_type_value(day , 'HY')
  
  se_q <- get_day_start_end(day , 'Q')
  quarter_end <- se_q$end
  quarter_value <- get_pd_type_value(day , 'Q')

  
  # ----- 停车费收费情况
  # 计算税率时，有实收按实收日期，无实收依次按冲抵日期、减免日期(若多次收款，按首次收款的日期)
  # 本期：本年初-本月末应收的停车费，截至当前的收费情况
  # 前期：截至本年之前应收的停车费，在本年之前未收的，于本年的收费情况
  # 实收数/（1+税率）
  
  # -- 计算本期应收、已收
  # 本期明细
  car_current_detail <- chargebills %>% 
    filter(cost_datestart >= ys_start ,
           cost_datestart <= month_end) %>% 
    distinct(project_name , pk_chargebills , parking_type , accrued_amount) %>% 
    # group_by(project_name , pk_chargebills , parking_type) %>% 
    # summarise(accrued_amount = sum(accrued_amount , na.rm = T)) %>% 
    left_join(gathering %>% 
                filter(cost_datestart >= ys_start ,
                       cost_datestart <= month_end ,
                       bill_date <= day) %>% 
                group_by(pk_chargebills) %>% 
                summarise(bill_date = min(bill_date , na.rm = T) ,
                          real_amount = sum(real_amount , na.rm = T))) %>% 
    left_join(relief %>% 
                filter(cost_datestart >= ys_start ,
                       cost_datestart <= month_end ,
                       enableddate <= day) %>% 
                group_by(pk_chargebills) %>% 
                summarise(enableddate = min(enableddate , na.rm = T) ,
                          adjust_amount = sum(adjust_amount , na.rm = T))) %>% 
    left_join(match %>% 
                filter(cost_datestart >= ys_start ,
                       cost_datestart <= month_end ,
                       virement_date <= day) %>% 
                group_by(pk_chargebills) %>% 
                summarise(virement_date = min(virement_date , na.rm = T) ,
                          match_amount = sum(match_amount , na.rm = T))) %>% 
    mutate(accrued_amount = round(replace_na(accrued_amount , 0),2) ,
           real_amount = round(replace_na(real_amount , 0),2) ,
           adjust_amount = round(replace_na(adjust_amount , 0),2) ,
           match_amount = round(replace_na(match_amount , 0),2) ,
           get_amount = round(real_amount + adjust_amount + match_amount , 2) ,
           c_use_date = coalesce(bill_date , virement_date , enableddate)) %>% 
    ungroup() %>% 
    # distinct(project_name , pk_chargebills , parking_type , c_use_date , 
    #          accrued_amount , get_amount) %>% 
    group_by(project_name , parking_type , c_use_date) %>% 
    summarise(accrued = sum(accrued_amount , na.rm = T) ,
              get_amount = sum(get_amount , na.rm = T)) %>% 
    ungroup() %>% 
    left_join(car_ratio) %>% 
    mutate(tax_ratio = replace_na(tax_ratio , 0) ,
           tax_start = if_else(is.na(tax_start) , as_date('1900-01-01') , as_date(tax_start)) ,
           tax_end = if_else(is.na(tax_end) , as_date('1900-01-01') , as_date(tax_end))) %>% 
    filter(tax_start <= c_use_date ,
           tax_end >= c_use_date)
    
  # 计算本期应收、已收
  car_current <- car_current_detail %>% 
    group_by(project_name) %>% 
    summarise(accrued_current = sum(accrued , na.rm = T) ,
              takeover_current = sum(get_amount , na.rm = T) ,
              accrued_current_tempark = sum(accrued[parking_type == '临停'] , na.rm = T) ,
              takeover_current_tempark = sum(get_amount[parking_type == '临停'] , na.rm = T) ,
              accrued_current_park = sum(accrued[parking_type == '非临停'] , na.rm = T) ,
              takeover_current_park = sum(get_amount[parking_type == '非临停'] , na.rm = T)) %>% 
    left_join(car_current_detail %>% 
                group_by(project_name , tax_ratio) %>% 
                summarise(accrued_current = sum(accrued , na.rm = T) ,
                          takeover_current = sum(get_amount , na.rm = T) ,
                          accrued_current_tempark = sum(accrued[parking_type == '临停'] , na.rm = T) ,
                          takeover_current_tempark = sum(get_amount[parking_type == '临停'] , na.rm = T) ,
                          accrued_current_park = sum(accrued[parking_type == '非临停'] , na.rm = T) ,
                          takeover_current_park = sum(get_amount[parking_type == '非临停'] , na.rm = T)) %>% 
                mutate(accrued_current_tax = round(accrued_current/(1+tax_ratio) , 2) ,
                       takeover_current_tax = round(takeover_current/(1+tax_ratio) , 2) ,
                       accrued_current_tempark_tax = round(accrued_current_tempark/(1+tax_ratio) , 2) ,
                       takeover_current_tempark_tax = round(takeover_current_tempark/(1+tax_ratio) , 2) ,
                       accrued_current_park_tax = round(accrued_current_park/(1+tax_ratio) , 2) ,
                       takeover_current_park_tax = round(takeover_current_park/(1+tax_ratio) , 2)) %>% 
                group_by(project_name) %>% 
                summarise(accrued_current_tax = sum(accrued_current_tax) ,
                          takeover_current_tax = sum(takeover_current_tax) ,
                          accrued_current_tempark_tax = sum(accrued_current_tempark_tax) ,
                          takeover_current_tempark_tax = sum(takeover_current_tempark_tax) ,
                          accrued_current_park_tax = sum(accrued_current_park_tax) ,
                          takeover_current_park_tax = sum(takeover_current_park_tax)))


  # -- 计算前期欠费应收、已收
  # 前期未收明细
  print(now())
  car_previous_detail <- chargebills %>% 
    filter(cost_datestart < ys_start) %>% 
    # group_by(project_name , pk_chargebills , parking_type) %>% 
    # summarise(accrued_amount = sum(accrued_amount , na.rm = T)) %>% 
    left_join(gathering %>% 
                filter(cost_datestart < ys_start) %>% 
                group_by(pk_chargebills) %>% 
                summarise(bill_date = min(bill_date , na.rm = T) ,
                          real = sum(real_amount[bill_date < ys_start] , na.rm = T) ,
                          real_amount_clear = sum(real_amount[bill_date >= ys_start] , na.rm = T))) %>% 
    left_join(relief %>% 
                filter(cost_datestart < ys_start) %>% 
                group_by(pk_chargebills) %>% 
                summarise(enableddate = min(enableddate , na.rm = T) ,
                          adjust = sum(adjust_amount[enableddate < ys_start] , na.rm = T) ,
                          adjust_amount_clear = sum(adjust_amount[enableddate >= ys_start] , na.rm = T))) %>% 
    left_join(match %>% 
                filter(cost_datestart < ys_start) %>% 
                group_by(pk_chargebills) %>% 
                summarise(virement_date = min(virement_date , na.rm = T) ,
                          match = sum(match_amount[virement_date < ys_start] , na.rm = T) ,
                          match_amount_clear = sum(match_amount[virement_date >= ys_start] , na.rm = T))) %>% 
    mutate(accrued_amount = round(replace_na(accrued_amount , 0),2) ,
           real = round(replace_na(real , 0),2) ,
           real_amount_clear = round(replace_na(real_amount_clear , 0),2) ,
           adjust = round(replace_na(adjust , 0),2) ,
           adjust_amount_clear = round(replace_na(adjust_amount_clear , 0),2) ,
           match = round(replace_na(match , 0),2) ,
           match_amount_clear = round(replace_na(match_amount_clear , 0),2) ,
           owe_amount = round(accrued_amount - real - adjust - match , 2) ,
           clear_amount = round(real_amount_clear + adjust_amount_clear + match_amount_clear,2) ,
           c_use_date = coalesce(bill_date , virement_date , enableddate)) %>% 
    ungroup() %>% 
    filter(owe_amount > 0) %>% 
    group_by(project_name , parking_type , c_use_date) %>%
    summarise(accrued = sum(owe_amount , na.rm = T) ,
              get_amount = sum(clear_amount , na.rm = T)) %>%
    ungroup() %>%
    left_join(car_ratio) %>%
    mutate(tax_ratio = replace_na(tax_ratio , 0) ,
           tax_start = if_else(is.na(tax_start) , as_date('1900-01-01') , as_date(tax_start)) ,
           tax_end = if_else(is.na(tax_end) , as_date('1900-01-01') , as_date(tax_end))) %>%
    filter(tax_start <= c_use_date ,
           tax_end >= c_use_date)
  print(now())
  
  # 计算前期欠费应收、已收
  car_previous <- car_previous_detail %>% 
    group_by(project_name) %>% 
    summarise(accrued_previous = sum(accrued , na.rm = T) ,
              takeover_previous = sum(get_amount , na.rm = T) ,
              accrued_previous_tempark = sum(accrued[parking_type == '临停'] , na.rm = T) ,
              takeover_previous_tempark = sum(get_amount[parking_type == '临停'] , na.rm = T) ,
              accrued_previous_park = sum(accrued[parking_type == '非临停'] , na.rm = T) ,
              takeover_previous_park = sum(get_amount[parking_type == '非临停'] , na.rm = T)) %>% 
    left_join(car_previous_detail %>% 
                group_by(project_name , tax_ratio) %>% 
                summarise(accrued_previous = sum(accrued , na.rm = T) ,
                          takeover_previous = sum(get_amount , na.rm = T) ,
                          accrued_previous_tempark = sum(accrued[parking_type == '临停'] , na.rm = T) ,
                          takeover_previous_tempark = sum(get_amount[parking_type == '临停'] , na.rm = T) ,
                          accrued_previous_park = sum(accrued[parking_type == '非临停'] , na.rm = T) ,
                          takeover_previous_park = sum(get_amount[parking_type == '非临停'] , na.rm = T)) %>% 
                mutate(accrued_previous_tax = round(accrued_previous/(1+tax_ratio) , 2) ,
                       takeover_previous_tax = round(takeover_previous/(1+tax_ratio) , 2) ,
                       accrued_previous_tempark_tax = round(accrued_previous_tempark/(1+tax_ratio) , 2) ,
                       takeover_previous_tempark_tax = round(takeover_previous_tempark/(1+tax_ratio) , 2) ,
                       accrued_previous_park_tax = round(accrued_previous_park/(1+tax_ratio) , 2) ,
                       takeover_previous_park_tax = round(takeover_previous_park/(1+tax_ratio) , 2)) %>% 
                group_by(project_name) %>% 
                summarise(accrued_previous_tax = sum(accrued_previous_tax) ,
                          takeover_previous_tax = sum(takeover_previous_tax) ,
                          accrued_previous_tempark_tax = sum(accrued_previous_tempark_tax) ,
                          takeover_previous_tempark_tax = sum(takeover_previous_tempark_tax) ,
                          accrued_previous_park_tax = sum(accrued_previous_park_tax) ,
                          takeover_previous_park_tax = sum(takeover_previous_park_tax)))


  # 合并本期及前期欠费
  car_bind <- car_current %>% 
    full_join(car_previous) 
  
  # 替换空值
  car_bind[is.na(car_bind)] <- 0
  
  # 计算
  car_bind <- car_bind %>% 
    mutate(accrued_amount = accrued_current + accrued_previous ,
           takeover_amount = takeover_current + takeover_previous ,
           accrued_amount_tax = accrued_current_tax + accrued_previous_tax ,
           takeover_amount_tax = takeover_current_tax + takeover_previous_tax ,
           accrued_amount_tempark = accrued_current_tempark + accrued_previous_tempark ,
           takeover_amount_tempark = takeover_current_tempark + takeover_previous_tempark ,
           accrued_amount_park = accrued_current_park + accrued_previous_park ,
           takeover_amount_park = takeover_current_park + takeover_previous_park ,
           accrued_amount_tempark_tax = accrued_current_tempark_tax + accrued_previous_tempark_tax ,
           takeover_amount_tempark_tax = takeover_current_tempark_tax + takeover_previous_tempark_tax ,
           accrued_amount_park_tax = accrued_current_park_tax + accrued_previous_park_tax ,
           takeover_amount_park_tax = takeover_current_park_tax + takeover_previous_park_tax ,
           day = day ,
           ys_start = ys_start ,
           ys_end = month_end ,
           get_end = month_end ,
           pd_type = 'M' ,
           pd_type_value = month_value ,
           is_complete = if_else(day == month_end , 1 , 0))
    
  # 设置重复
  car <- car_bind %>% 
    rbind(car_bind %>% 
            mutate(ys_end = quarter_end ,
                   get_end = quarter_end ,
                   pd_type = 'Q' ,
                   pd_type_value = quarter_value ,
                   is_complete = if_else(day == quarter_end , 1 , 0))) %>% 
    rbind(car_bind %>% 
            mutate(ys_end = halfyear_end ,
                   get_end = halfyear_end ,
                   pd_type = 'HY' ,
                   pd_type_value = halfyear_value ,
                   is_complete = if_else(day == halfyear_end , 1 , 0))) %>% 
    rbind(car_bind %>% 
            mutate(ys_end = year_end ,
                   get_end = year_end ,
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
  dbWriteTable(conn , table , car , append = T , row.names = F)
  print('write car data success')
  
  # 断开连接
  dbDisconnect(conn)
  
}

