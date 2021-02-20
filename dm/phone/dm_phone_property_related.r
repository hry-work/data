# 本地使用
# source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')
# 调度使用
source('/root/data/env_centos.r' , encoding = 'utf8')

author <- c('huruiyi')
table <- 'dm_phone_property_related'

for (day in days) {
  
  day <- as_date(day)
  print(day)
  
  # ---------- 从sql server取物业费相关表
  # 应收
  print(paste0('应收开始：' , now()))
  chargebills <- sqlQuery(con_sqls , glue("select belong , project1 , project2 , 
                                          project3 , project4 , pk_project , project_name , 
                                          pk_house , house_code , house_name , wy_cycle , 
                                          pk_chargebills , cost_datestart , accrued_amount , projectname
                                          from mid_eve_fee_property_chargebills
                                          where cost_datestart <= '{year_end}'")) %>% 
    filter(!is.na(project_name) , 
           !project_name %in% c('测试项目' , '北京菊源里') ,
           accrued_amount > 0) %>% 
    mutate(cost_datestart = as_date(cost_datestart) ,
           pk_chargebills = trimws(pk_chargebills))
  print(paste0('应收结束：' , now()))
  
  # 实收
  print(paste0('实收开始：' , now()))
  gathering <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                        cost_datestart , real_amount , bill_date
                                        from mid_eve_fee_property_gathering
                                        where cost_datestart <= '{year_end}'")) %>% 
    filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
    mutate(cost_datestart = as_date(cost_datestart) ,
           bill_date = as_date(bill_date) ,
           pk_chargebills = trimws(pk_chargebills))
  print(paste0('实收结束：' , now()))
  
  # 减免
  print(paste0('减免开始：' , now()))
  relief <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                     cost_datestart , adjust_amount , enableddate
                                     from mid_eve_fee_property_relief
                                     where cost_datestart <= '{year_end}'")) %>% 
    filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
    mutate(cost_datestart = as_date(cost_datestart) ,
           enableddate = as_date(enableddate) ,
           pk_chargebills = trimws(pk_chargebills))
  print(paste0('减免结束：' , now()))
  
  # 冲抵
  print(paste0('冲抵开始：' , now()))
  match <- sqlQuery(con_sqls , glue("select project_name , pk_house , pk_chargebills , 
                                    cost_datestart , match_amount , bill_date
                                    from mid_eve_fee_property_match
                                    where cost_datestart <= '{year_end}'")) %>% 
    filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
    mutate(cost_datestart = as_date(cost_datestart) ,
           bill_date = as_date(bill_date) ,
           pk_chargebills = trimws(pk_chargebills))
  print(paste0('冲抵结束：' , now()))
  
  
  # ---------- 物业费涉及日期计算
  property_date <- chargebills %>% 
    distinct(cost_datestart) %>% 
    mutate(cost_datestart = as_date(cost_datestart))
  
  
  # ---------- 物业费户、金额
  # 今年应收的物业费，在今年及之前的收缴情况
  print(now())
  property <- chargebills %>% 
    filter(cost_datestart >= year_start ,
           cost_datestart <= year_end) %>% 
    left_join(gathering %>% 
                filter(cost_datestart >= year_start ,
                       cost_datestart <= year_end ,
                       bill_date <= day) %>% 
                group_by(pk_chargebills) %>% 
                summarise(real_amount = sum(real_amount , na.rm = T))) %>% 
    left_join(relief %>% 
                filter(cost_datestart >= year_start ,
                       cost_datestart <= year_end ,
                       enableddate <= day) %>% 
                group_by(pk_chargebills) %>% 
                summarise(adjust_amount = sum(adjust_amount , na.rm = T))) %>% 
    left_join(match %>% 
                filter(cost_datestart >= year_start ,
                       cost_datestart <= year_end ,
                       bill_date <= day) %>% 
                group_by(pk_chargebills) %>% 
                summarise(match_amount = sum(match_amount , na.rm = T))) %>% 
    mutate(real_amount = round(replace_na(real_amount , 0),2) ,
           adjust_amount = round(replace_na(adjust_amount , 0),2) ,
           match_amount = round(replace_na(match_amount , 0),2) ,
           get_amount = round(real_amount + adjust_amount + match_amount,2) ,
           owe_amount = round(accrued_amount - real_amount - adjust_amount - match_amount,2))
  print(now())
  
  print(now())
  # 金额
  property_amount <- property %>% 
    group_by(project_name) %>% 
    summarise(accrued_amount1 = sum(accrued_amount , na.rm = T) ,
              takeover_amount = sum(get_amount , na.rm = T) ,
              accrued_amount_business = sum(accrued_amount[projectname == '商业物业费'] , na.rm = T) ,
              takeover_amount_business = sum(get_amount[projectname == '商业物业费'] , na.rm = T) ,
              accrued_amount_house = sum(accrued_amount[projectname == '住宅物业费'] , na.rm = T) ,
              takeover_amount_house = sum(get_amount[projectname == '住宅物业费'] , na.rm = T) ,
              accrued_amount_office = sum(accrued_amount[projectname == '写字楼物业费'] , na.rm = T) ,
              takeover_amount_office = sum(get_amount[projectname == '写字楼物业费'] , na.rm = T) ,
              accrued_amount_pubfacilities = sum(accrued_amount[projectname == '公建配套物业费'] , na.rm = T) ,
              takeover_amount_pubfacilities = sum(get_amount[projectname == '公建配套物业费'] , na.rm = T))
  
  print(now())

  # 户
  property_house <- property %>% 
    group_by(project_name , pk_house) %>% 
    summarise(owe_amount = sum(owe_amount)) %>% 
    group_by(project_name) %>% 
    summarise(house_cnt = n_distinct(pk_house) ,
              takeover_cnt = n_distinct(pk_house[owe_amount <= 0])) %>% 
    left_join(property %>% 
                group_by(project_name , pk_house , projectname) %>% 
                summarise(owe_amount = sum(owe_amount)) %>% 
                group_by(project_name) %>% 
                summarise(house_cnt_business = n_distinct(pk_house[projectname == '商业物业费'] , na.rm = T) ,
                          takeover_cnt_business = n_distinct(pk_house[owe_amount <= 0 & projectname == '商业物业费'] , na.rm = T) ,
                          house_cnt_house = n_distinct(pk_house[projectname == '住宅物业费'] , na.rm = T) ,
                          takeover_cnt_house = n_distinct(pk_house[owe_amount <= 0 & projectname == '住宅物业费'] , na.rm = T) ,
                          house_cnt_office = n_distinct(pk_house[projectname == '写字楼物业费'] , na.rm = T) ,
                          takeover_cnt_office = n_distinct(pk_house[owe_amount <= 0 & projectname == '写字楼物业费'] , na.rm = T) ,
                          house_cnt_pubfacilities = n_distinct(pk_house[projectname == '公建配套物业费'] , na.rm = T) ,
                          takeover_cnt_pubfacilities = n_distinct(pk_house[owe_amount <= 0 & projectname == '公建配套物业费'] , na.rm = T)))
  print(now())

  
  # ---------- 清欠
  # 应收：每年1日之前的应收，在该年1日之前未收的
  # 清收：清欠应收截至该年末/半年末/季末/月末的清收的金额(跑截至到每个月月末的，特定月份的月末即为年末、半年末、季末)
  
  # 截至本年初的欠费(饱和清欠)
  print(now())
  saturation_recovery <- chargebills %>% 
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
           owe_amount = round(accrued_amount - real_amount - adjust_amount - match_amount,2)) %>% 
    # filter(owe_amount > 0) %>% 此条注掉，因历史数据部分存在错位，即上个月欠费为负，本月欠费为正，刚好充消
    group_by(project_name) %>% 
    summarise(saturation_recovery = sum(owe_amount))
  print(now())
  
  # 今年截至日期所属月末的清欠
  recovery <- gathering %>% 
    filter(cost_datestart < year_start ,
           bill_date >= year_start , bill_date <= day) %>% 
    group_by(project_name) %>% 
    summarise(real_amount = sum(real_amount , na.rm = T)) %>% 
    full_join(relief %>% 
                filter(cost_datestart < year_start ,
                       enableddate >= year_start , enableddate <= day) %>% 
                group_by(project_name) %>% 
                summarise(adjust_amount = sum(adjust_amount , na.rm = T))) %>% 
    full_join(match %>% 
                filter(cost_datestart < year_start ,
                       bill_date >= year_start , bill_date <= day) %>% 
                group_by(project_name) %>% 
                summarise(match_amount = sum(match_amount , na.rm = T))) %>% 
    mutate(real_amount = replace_na(real_amount , 0) ,
           adjust_amount = replace_na(adjust_amount , 0) ,
           match_amount = replace_na(match_amount , 0) ,
           recovery = round(real_amount + adjust_amount + match_amount , 2)) %>% 
    select(project_name , recovery) 
  
  # 合并清欠数据
  recovered <- saturation_recovery %>% 
    left_join(recovery) %>% 
    mutate(recovery = replace_na(recovery , 0))
  
  
  # ---------- 合并物业费及清欠数据，再判断日期设置重复(截至年、半年、季度、月末的数据)
  # 合并
  property_related <- property_amount %>% 
    left_join(property_house) %>% 
    full_join(recovered) %>% 
    mutate(day = day ,
           ys_start = year_start ,
           ys_end = year_end ,
           get_end = month_end ,
           pd_type = 'M' ,
           pd_type_value = month_value ,
           is_complete = if_else(day == month_end , 1 , 0)) %>% 
    rename(accrued_amount = accrued_amount1)
  
  # 替换空值
  property_related[is.na(property_related)] <- 0
  
  # 设置重复
  property_related <- property_related %>% 
    rbind(property_related %>% 
            mutate(get_end = quarter_end ,
                   pd_type = 'Q' ,
                   pd_type_value = quarter_value ,
                   is_complete = if_else(day == quarter_end , 1 , 0))) %>% 
    rbind(property_related %>% 
            mutate(get_end = halfyear_end ,
                   pd_type = 'HY' ,
                   pd_type_value = halfyear_value ,
                   is_complete = if_else(day == halfyear_end , 1 , 0))) %>% 
    rbind(property_related %>% 
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
  dbWriteTable(conn , table , property_related , append = T , row.names = F)
  print('write success')
  
  # 断开连接
  dbDisconnect(conn)
  
}

