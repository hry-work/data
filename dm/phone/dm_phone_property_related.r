# 本地使用
source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')
# 调度使用
# source('/root/data/env_centos.r' , encoding = 'utf8')

author <- c('huruiyi')
print(author)
table <- 'dm_phone_property_related'

for (day in days) {
  
  day <- as_date(day)
  
  # ---------- 从sql server取物业费相关表
  # 应收
  print(paste0('应收开始：' , now()))
  chargebills <- sqlQuery(con_sqls , glue("select belong , project1 , project2 , 
                                          project3 , project4 , pk_project , project_name , 
                                          pk_house , house_code , house_name , wy_cycle , 
                                          pk_chargebills , cost_datestart , accrued_amount , projectname
                                          from mid_eve_fee_property_chargebills
                                          where cost_datestart <= '{year_end}'")) %>% 
    filter(!is.na(project_name) , !project_name %in% c('测试项目' , '北京菊源里')) %>% 
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
  
  # 最早、晚的物业费应收日期
  property_start <- min(property_date$cost_datestart)
  
  # map_date
  map_date <- sqlQuery(con_sqls , glue("select day , month_start , month_end , year_start , year_end ,
                                     year_start_thb , year_end_thb
                                     from mid_map_date
                                     where day between '{property_start}' and '{year_end}'"))
  
  
  # ---------- 物业费户、金额
  # 今年应收的物业费，在今年及之前的收缴
  print(now())
  property <- chargebills %>% 
    filter(cost_datestart >= year_start ,
           cost_datestart <= year_end) %>% 
    left_join(gathering %>% 
                filter(cost_datestart >= year_start ,
                       cost_datestart <= year_end ,
                       bill_date <= month_end) %>% 
                group_by(pk_chargebills) %>% 
                summarise(real_amount = sum(real_amount , na.rm = T))) %>% 
    left_join(relief %>% 
                filter(cost_datestart >= year_start ,
                       cost_datestart <= year_end ,
                       enableddate <= month_end) %>% 
                group_by(pk_chargebills) %>% 
                summarise(adjust_amount = sum(adjust_amount , na.rm = T))) %>% 
    left_join(match %>% 
                filter(cost_datestart >= year_start ,
                       cost_datestart <= year_end ,
                       bill_date <= month_end) %>% 
                group_by(pk_chargebills) %>% 
                summarise(match_amount = sum(match_amount , na.rm = T))) %>% 
    mutate(real_amount = round(replace_na(real_amount , 0),2) ,
           adjust_amount = round(replace_na(adjust_amount , 0),2) ,
           match_amount = round(replace_na(match_amount , 0),2) ,
           get_amount = round(real_amount + adjust_amount + match_amount,2) ,
           owe_amount = round(accrued_amount - real_amount - adjust_amount - match_amount,2))
  print(now())
  
  print(now())
  property_amount <- property %>% 
    group_by(project_name) %>% 
    summarise(accrued_amount = sum(accrued_amount , na.rm = T) ,
              takeover_amount = sum(get_amount , na.rm = T))
  
  property_house <- property %>% 
    group_by(project_name , pk_house) %>% 
    summarise(owe_amount = sum(owe_amount)) %>% 
    group_by(project_name) %>% 
    summarise(house_cnt = n_distinct(pk_house) ,
              get_cnt = n_distinct(pk_house[owe_amount <= 0]))
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
  
  # 合并清欠数据
  recovered <- saturation_recovery %>% 
    left_join(recovery) %>% 
    mutate(recovery = replace_na(recovery , 0))
  
  # 判断日期，设置重复(截至年、半年、季度、月末的数据)
  

  # # 连接mysql，写入数据
  # 
  # # 连接mysql dm层
  # conn <- dbConnect(con_dm)
  # 
  # # test_2 <- dbGetQuery(conn , "select * from test")
  # 
  # print('start write')
  # 
  # 删除
  # 
  # # 写入
  # dbWriteTable(conn , 'test' , test_write , append = T , row.names = F)
}

