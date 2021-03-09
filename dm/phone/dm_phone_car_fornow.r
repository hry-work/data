# 本地使用
# source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')
# 调度使用
source('/root/data/env_centos.r' , encoding = 'utf8')
# 停车费
# 当前停车费模式存在过几个月后生成之前月份的应收(收款时才生成应收)，因此设置为全量
# 当前只有21年的预算，因此先只有21年的车费的预算和达成+历史的达成
# 根据预算来看预算达成时，完全按现金流，只看实收，不看减免和冲抵。因此不再看是本期应收还是前期欠费应收。

author <- c('huruiyi')
table <- 'dm_phone_car'

# ---------- 从sql server取停车费相关表
# 预算应收(不含税)
budget_data <- sqlQuery(con_sqls , glue("select project_name , month_start , month , budget_amount
                                         from mid_dim_budget_parking
                                         where year = '2021' and is_sys_project = 1 and tax = 0"))

# 实收
print(paste0('停车费收费开始：' , now()))
car_data <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                      cost_datestart , accrued_amount , real_amount , 
                                      bill_date , projectname
                                      from mid_eve_fee_parking_gathering")) %>% 
  filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
  mutate(cost_datestart = as_date(cost_datestart) ,
         bill_date = as_date(bill_date) ,
         pk_chargebills = trimws(pk_chargebills) ,
         project_type = if_else(grepl('临时' , projectname) , '临停' , '非临停' , '非临停')) %>% 
  filter(!is.na(bill_date))

print(paste0('停车费收费结束：' , now()))


# 税率表
# 按实收月份的税率，不按应收
car_ratio <- sqlQuery(con_sqls , glue("select project_name , tax_ratio , tax_start , tax_end
                                       from mid_dim_tax_ratio
                                       where tax_type = 'parking'"))


# ---------- 停车费日期计算
car_date <- car_data %>% 
  distinct(bill_date) %>% 
  filter(!is.na(bill_date)) %>% 
  mutate(bill_date = as_date(bill_date)) 

# 停车费收费截止月末
date <- sqlQuery(con_sqls , "select day , month_end from mid_map_date") %>% 
  mutate(day = as_date(day) ,
         month_end = as_date(month_end)) %>% 
  filter(day >= min(car_date$bill_date) , 
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
  # 当前预算模式下，计算税率时按实收日期对应的税率
  # 实收数/（1+税率）
  
  # -- 计算今年收的本期及前期应收
  car_detail <- car_data %>% # 本年度收费
    filter(bill_date >= ys_start ,
           bill_date <= month_end) %>% 
    group_by(project_name , project_type , pk_chargebills) %>% 
    summarise(bill_date = min(bill_date) ,
              real = sum(real_amount[bill_date <= month_end]) , #本年收费
              real_current = sum(real_amount[cost_datestart >= ys_start & bill_date <= month_end]) , #本年收的本年的
              real_previous = sum(real_amount[cost_datestart < ys_start & bill_date <= month_end])) %>%  #本年收的历史年的
    left_join(car_ratio) %>% 
    mutate(tax_ratio = replace_na(tax_ratio , 0) ,
           tax_start = if_else(is.na(tax_start) , as_date('1900-01-01') , as_date(tax_start)) ,
           tax_end = if_else(is.na(tax_end) , as_date('3000-01-01') , as_date(tax_end))) %>% 
    filter(tax_start <= bill_date ,
           tax_end >= bill_date) %>% 
    group_by(project_name , project_type , tax_ratio) %>% 
    summarise(real = sum(real) ,
              real_current = sum(real_current) ,
              real_previous = sum(real_previous)) %>% 
    mutate(real_tax = round(real/(1+tax_ratio) , 2) , 
           real_current_tax = round(real_current/(1+tax_ratio) , 2) ,
           real_previous_tax = round(real_previous/(1+tax_ratio) , 2)) %>% 
    group_by(project_name) %>% 
    summarise(takeover_amount = sum(real) ,
              takeover_amount_tempark = sum(real[project_type == '临停']) , 
              takeover_amount_park = sum(real[project_type == '非临停']) , 
              takeover_amount_tax = sum(real_tax) ,
              takeover_amount_tempark_tax = sum(real_tax[project_type == '临停']) , 
              takeover_amount_park_tax = sum(real_tax[project_type == '非临停']) , 
              takeover_current = sum(real_current) ,
              takeover_current_tempark = sum(real_current[project_type == '临停']) , 
              takeover_current_park = sum(real_current[project_type == '非临停']) , 
              takeover_current_tax = sum(real_current_tax) ,
              takeover_current_tempark_tax = sum(real_current_tax[project_type == '临停']) , 
              takeover_current_park_tax = sum(real_current_tax[project_type == '非临停']) , 
              takeover_previous = sum(real_previous) ,
              takeover_previous_tempark = sum(real_previous[project_type == '临停']) , 
              takeover_previous_park = sum(real_previous[project_type == '非临停']) , 
              takeover_previous_tax = sum(real_previous_tax) ,
              takeover_previous_tempark_tax = sum(real_previous_tax[project_type == '临停']) , 
              takeover_previous_park_tax = sum(real_previous_tax[project_type == '非临停'])) %>% 
    full_join(budget_data %>% #不含税的停车费预算应达成
                filter(month_start >= year_start ,
                       month_start <= day) %>% 
                group_by(project_name) %>% 
                summarise(accrued_amount_tax = sum(budget_amount))) %>% 
    mutate(day = day ,
           ys_start = ys_start ,
           ys_end = month_end ,
           get_end = month_end ,
           pd_type = 'M' ,
           pd_type_value = month_value ,
           is_complete = if_else(day == month_end , 1 , 0) ,
           accrued_amount = 0 ,
           accrued_amount_park = 0 ,
           accrued_amount_park_tax = 0 ,
           accrued_amount_tempark = 0 ,
           accrued_amount_tempark_tax = 0 ,
           accrued_current = 0 ,
           accrued_current_park = 0 ,
           accrued_current_park_tax = 0 ,
           accrued_current_tax = 0 ,
           accrued_current_tempark = 0 ,
           accrued_current_tempark_tax = 0 ,
           accrued_previous = 0 ,
           accrued_previous_park = 0 ,
           accrued_previous_park_tax = 0 ,
           accrued_previous_tax = 0 ,
           accrued_previous_tempark = 0 ,
           accrued_previous_tempark_tax = 0)

  if(nrow(car_detail) > 0){
    
    # 替换空值
    car_detail[is.na(car_detail)] <- 0
    
    
    # 设置重复
    car <- car_detail %>% 
      rbind(car_detail %>% 
              mutate(ys_end = quarter_end ,
                     get_end = quarter_end ,
                     pd_type = 'Q' ,
                     pd_type_value = quarter_value ,
                     is_complete = if_else(day == quarter_end , 1 , 0))) %>% 
      rbind(car_detail %>% 
              mutate(ys_end = halfyear_end ,
                     get_end = halfyear_end ,
                     pd_type = 'HY' ,
                     pd_type_value = halfyear_value ,
                     is_complete = if_else(day == halfyear_end , 1 , 0))) %>% 
      rbind(car_detail %>% 
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
  
  
}

