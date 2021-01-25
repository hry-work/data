# 本地使用
# source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')
# 调度使用
source('/root/data/env_centos.r' , encoding = 'utf8')
# 停车费

author <- c('huruiyi')
table <- 'dm_phone_car'

for (day in days) {
  
  day <- as_date(day)
  print(day)

  # ---------- 从sql server取停车费相关表
  # 停车费收费表
  print(paste0('停车费收费开始：' , now()))
  car_data <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                        cost_datestart , accrued_amount , real_amount , 
                                        bill_date , projectname
                                        from mid_eve_fee_parking_gathering
                                        where cost_datestart between '{year_start}' and '{year_end}'")) %>% 
    filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
    mutate(cost_datestart = as_date(cost_datestart) ,
           bill_date = as_date(bill_date) ,
           pk_chargebills = trimws(pk_chargebills))
  print(paste0('停车费收费结束：' , now()))
  
  # 税率表
  car_ratio <- sqlQuery(con_sqls , glue("select project_name , tax_ratio
                                         from mid_dim_tax_ratio
                                         where tax_type = 'parking'"))
  
  # 停车费收费情况
  # 本年初-本月末应收的停车费，截至本月末的收费情况
  car <- car_data %>% 
    filter(cost_datestart <= month_end) %>% 
    distinct(project_name , pk_chargebills , accrued_amount) %>% 
    group_by(project_name , pk_chargebills) %>% 
    summarise(accrued_amount = sum(accrued_amount , na.rm = T)) %>% 
    left_join(car_data %>% 
                filter(cost_datestart <= month_end) %>% 
                group_by(project_name , pk_chargebills) %>% 
                summarise(real_amount = sum(real_amount[bill_date <= month_end] , na.rm = T))) %>% 
    group_by(project_name) %>% 
    summarise(accrued_amount = sum(accrued_amount , na.rm = T) ,
              takeover_amount = sum(real_amount , na.rm = T)) %>% 
    left_join(car_ratio) %>% 
    mutate(tax_ratio = replace_na(tax_ratio , 0) ,
           accrued_amount_tax = round(accrued_amount*(1-tax_ratio) , 2) ,
           takeover_amount_tax = round(takeover_amount*(1-tax_ratio) , 2) ,
           day = day ,
           ys_start = year_start ,
           ys_end = month_end ,
           get_end = month_end ,
           pd_type = 'M' ,
           pd_type_value = month_value ,
           is_complete = if_else(day == month_end , 1 , 0))

  # 替换空值
  car[is.na(car)] <- 0
    
  # 设置重复
  car <- car %>% 
    rbind(car %>% 
            mutate(ys_end = quarter_end ,
                   get_end = quarter_end ,
                   pd_type = 'Q' ,
                   pd_type_value = quarter_value ,
                   is_complete = if_else(day == quarter_end , 1 , 0))) %>% 
    rbind(car %>% 
            mutate(ys_end = halfyear_end ,
                   get_end = halfyear_end ,
                   pd_type = 'HY' ,
                   pd_type_value = halfyear_value ,
                   is_complete = if_else(day == halfyear_end , 1 , 0))) %>% 
    rbind(car %>% 
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
  
}
