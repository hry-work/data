# 本地使用
source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')
# 调度使用
# source('/root/data/env_centos.r' , encoding = 'utf8')

author <- c('huruiyi')
print(author)
table <- 'dm_phone_property_related'

for (day in DAYS) {
  
  day <- as_date(day)
  year_start <- as_date(year_start)
  year_end <- as_date(year_end)
  
  # ---------- 从sql server取物业费相关表
  # 应收
  print(paste0('应收开始：' , now()))
  chargebills <- sqlQuery(con_sqls , glue("select belong , project1 , project2 , 
                                          project3 , project4 , pk_project , project_name , 
                                          pk_house , house_code , house_name , wy_cycle , 
                                          pk_chargebills , cost_datestart , accrued_amount , projectname
                                          from mid_eve_fee_property_chargebills
                                          where cost_datestart <= '{year_end}'"))
  print(paste0('应收结束：' , now()))
  
  # 实收
  print(paste0('实收开始：' , now()))
  gathering <- sqlQuery(con_sqls , glue("select pk_house , pk_chargebills , 
                                        cost_datestart , real_amount , bill_date
                                        from mid_eve_fee_property_gathering
                                        where cost_datestart <= '{year_end}'"))
  print(paste0('实收结束：' , now()))
  
  # 减免
  print(paste0('减免开始：' , now()))
  relief <- sqlQuery(con_sqls , glue("select pk_house , pk_chargebills , cost_datestart , 
                                     adjust_amount , enableddate
                                     from mid_eve_fee_property_relief
                                     where cost_datestart <= '{year_end}'"))
  print(paste0('减免结束：' , now()))
  
  # 冲抵
  print(paste0('冲抵开始：' , now()))
  match <- sqlQuery(con_sqls , glue("select pk_house , pk_chargebills , cost_datestart , 
                                    match_amount , bill_date
                                    from mid_eve_fee_property_match
                                    where cost_datestart <= '{year_end}'"))
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
  # 年、半年、季度、月
  
  
  # ---------- 清欠
  # 应收：每年1日之前的应收，在该年1日之前未收的
  # 清收：清欠应收截至该年末/半年末/季末/月末的清收的金额(跑截至到每个月月末的，特定月份的月末即为年末、半年末、季末)
  for (i in 2:nrow(property_year)) {
    
    ys_start <- as_date(paste0(property_year$property_year[i] , '-01-01'))
    ys_end <- as_date(paste0(property_year$property_year[i] , '-12-31'))
    print(paste0(ys_start , ' ' , ys_end))
    
    # 截至本年初的欠费(饱和清欠)
    # saturation_liquidate <- 
    
    # 截至每月末的清欠(再计算出每个月的清欠额)
    
    # liquidate_data <- bind_rows(liquidate_data , liquidate)
    
  }
  
  
  
  # # 连接mysql，写入数据
  # 
  # # 连接mysql dm层
  # conn <- dbConnect(con_dm)
  # 
  # # test_2 <- dbGetQuery(conn , "select * from test")
  # 
  # print('start write')
  # 
  # # 写入
  # dbWriteTable(conn , 'test' , test_write , append = T , row.names = F)
}

