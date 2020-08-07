source('env.r' , encoding = 'utf8')

author <- c('huruiyi')
table <- 'mid_eve_finance_fee_property'      # 物业费事件表（已修正的）
# 命名规则：表应归属库_表类型_归属部门_业务大类_表详细归类

# 本年末
year_end <- as_date(paste0(year(day) , '-12-31'))

# -------------------- 准备基础数据 --------------------

# ---------- 物业费code
property_code <- dbGetQuery(con_orc , glue("select distinct pk_projectid , projectcode , projectname
                                            from wy_bd_fmproject 
                                            where projectcode in ('001','002','003','004')"))

# ---------- 项目基础数据
basic_info <- sqlQuery(con_sql , "select pk_house , house_name , pk_floor , floor_name , 
                                  pk_unit , unit_name , pk_build , build_name , 
                                  pk_project , project_name , client_name
                                  from dim_owner_basic_info")

# ---------- 项目归属区域
belong <- dbGetQuery(con_orc , glue("select porject1 , porject2 , porject3 , porject4 , porject6 , wy_cycle
                                    from xywy_project"))

# ---------- 收费类型
gathering_type <- dbGetQuery(con_orc , glue("select pk_gatheringtype , code , name as gatheringtype_name
                                            from wy_bd_gatheringtype
                                            where dr = 0 "))


# 下方明细表较大，设置每执行一个后清除内存并等待2分钟
print(paste0('应收开始：' , now()))
# ---------- 应收明细表
# 一个pk_chargebills只会有一条记录
chargebills <- dbGetQuery(con_orc , glue("select pk_chargebills , pk_house , pk_projectid , 
                                          cost_date , cost_startdate , cost_enddate , 
                                          accrued_amount , dr
                                          from wy_bill_chargebills"))
print(paste0('应收结束：' , now()))
gc()
Sys.sleep(120)

print(paste0('实收1开始：' , now()))
# ---------- 实收明细表
gathering <- dbGetQuery(con_orc , glue("select pk_gathering , pk_client , bill_date , dr
                                        from wy_bill_gathering"))
print(paste0('实收1结束：' , now()))
gc()
Sys.sleep(120)

print(paste0('实收2开始：' , now()))
gathering_d <- dbGetQuery(con_orc , glue("select pk_gathering , pk_gathering_d , source , 
                                          souse_type , pk_gathering_type , real_amount , dr
                                          from wy_bill_gathering_d"))
print(paste0('实收2结束：' , now()))
gc()
Sys.sleep(120)

print(paste0('减免1开始：' , now()))
# ---------- 减免明细表
receive <- dbGetQuery(con_orc , glue("select pk_receivable , enableddate , enabled_state , adjust_type , dr
                                      from wy_bd_receivable"))
print(paste0('减免1结束：' , now()))
gc()
Sys.sleep(120)

print(paste0('减免2开始：' , now()))
receive_d <- dbGetQuery(con_orc , glue("select pk_receivable , pk_receivable_d , 
                                        pk_chargebills , adjust_amount , dr
                                        from wy_bd_receivable_d"))
print(paste0('减免2结束：' , now()))
gc()
Sys.sleep(120)

print(paste0('冲抵开始：' , now()))
# ---------- 冲抵明细表
matchforward <- dbGetQuery(con_orc , glue("select pk_forward , pk_recerive , 
                                           pk_gahtering_d , match_amount , dr
                                           from wy_virement_matchforward"))
print(paste0('冲抵结束：' , now()))
gc()
Sys.sleep(120)


# -------------------- 基础数据合并 --------------------
print(now())
eve_property <- chargebills %>%   #应收
  mutate(COST_STARTDATE = as_date(COST_STARTDATE) ,
         COST_ENDDATE = as_date(COST_ENDDATE)) %>% 
  filter(DR == 0 , #COST_ENDDATE >= COST_STARTDATE , 
         (COST_STARTDATE <= year_end | COST_ENDDATE <= year_end)) %>% 
  inner_join(property_code , by = 'PK_PROJECTID') %>% 
  left_join(basic_info , by = c('PK_HOUSE' = 'pk_house')) %>% 
  left_join(belong , by = c('project_name' = 'PORJECT6')) %>% 
  select(-DR) %>% 
  left_join(gathering %>%   #实收
              rename(bill_time = BILL_DATE) %>% 
              mutate(bill_date = as_date(bill_time) ,
                     bill_time = as_datetime(bill_time)) %>% 
              filter(DR == 0 , bill_date <= year_end) %>% 
              select(-DR) %>% 
              inner_join(gathering_d %>% 
                           filter(DR == 0) %>% 
                           select(-DR) %>% 
                           rename(gather_souse_type = SOUSE_TYPE) , by = 'PK_GATHERING') , by = c('PK_CHARGEBILLS' = 'SOURCE')) %>% 
  left_join(gathering_type , by = c('PK_GATHERING_TYPE' = 'PK_GATHERINGTYPE')) %>% 
  left_join(receive %>%   #减免
              rename(enabledtime = ENABLEDDATE) %>% 
              mutate(enableddate = as_date(enabledtime) ,
                     enabledtime = as_datetime(enabledtime) ,
                     ENABLED_STATE = trimws(ENABLED_STATE)) %>% 
              filter(DR == 0 , ENABLED_STATE == '已启用' , ADJUST_TYPE == '实收' , enableddate <= year_end) %>% 
              select(-DR) %>% 
              left_join(receive_d %>% 
                          filter(DR == 0) %>% 
                          select(-DR) , by = c('PK_RECEIVABLE')) , by = c('PK_CHARGEBILLS')) %>% 
  left_join(matchforward %>%   #冲抵
              filter(DR == 0) %>% 
              select(-DR) %>% 
              rename(PK_GATHERING_D = PK_GAHTERING_D) %>% 
              inner_join(gathering_d %>% 
                           filter(DR == 0 , SOUSE_TYPE == '预收') %>% 
                           select(PK_GATHERING_D , SOUSE_TYPE) , by = 'PK_GATHERING_D') %>% 
              rename(pk_forward_d = PK_GATHERING_D), 
            by = c('PK_CHARGEBILLS' = 'PK_RECERIVE')) %>% 
  rename(gatheringtype_code = CODE ,
         forward_souse_type = SOUSE_TYPE) %>% 
  select(porject1 , porject2 , porject3 , porject4 , pk_project , project_name , pk_build , 
         build_name , pk_unit , unit_name , pk_floor , floor_name , pk_house , house_name ,  
         pk_chargebills , cost_date , cost_startdate , cost_enddate ,
         accrued_amount , real_amount , adjust_amount , match_amount ,  
         pk_projectid , projectcode , projectname , wy_cycle , pk_gathering , pk_gathering_d , 
         gather_souse_type , pk_gathering_type , gatheringtype_code , gatheringtype_name ,   
         bill_date , bill_time , pk_receivable , pk_receivable_d , adjust_type ,     
         enableddate , enabledtime , enabled_state , pk_forward , pk_forward_d ,    
         forward_souse_type , pk_client , client_name) %>% 
  mutate(d_t = now()) %>% 
  print(now())

names(eve_property) <- tolower(names(eve_property))

# names(eve_property)


# cs <- eve_property %>% 
#   filter(!is.na(enabledtime))



eve_property2 <- eve_property %>% 
  head(20) %>% 
  # mutate(bill_time = as_datetime(bill_time) ,
  #        enabledtime = as_datetime(enabledtime) ,
  #        d_t = as_datetime(d_t)) %>% 
  
  
  
  
  
  
  # 写入sql server
  # 使用此函数写入时，需注意在sql server中建好的表的字段类型，一定要适用数据，否则报错
# 使用此函数时，一定要保证数据库中列同此表输出列字段一致、顺序一致、类型匹配、字段长度满足要求，否则报错
# 此处设定append=TRUE，若为false，此函数会自行在数据库建表，类型修改麻烦，且若库中已有同名表会报错，因此建议设定append=TRUE
# 若为全量，先执行清空表的操作，再执行入库
sqlClear(con_sql, 'mid_eve_finance_fee_property')
sqlSave(con_sql , eve_property2 , tablename = "mid_eve_finance_fee_property" , 
        append = TRUE , rownames = FALSE , fast = FALSE)


# 清除工作区内容
rm(list = ls())

# 断开sql连接
close(con_sql)
close(con_orc)

# 清内存
gc()



# # 应收结束在开始之前（应收周期生成错误）
# wrong_1 <- eve_property %>% 
#   filter(COST_STARTDATE > COST_ENDDATE)
# 
# write.xlsx(wrong_1 , '..\\data\\mid\\eve\\物业费应收结束在开始之前数据.xlsx')
# 
# # 应收额为0（待确认是否金额生成）
# wrong_2 <- eve_property %>% 
#   filter(ACCRUED_AMOUNT <= 0)
# 
# write.xlsx(wrong_2 , '..\\data\\mid\\eve\\物业费应收小于0.xlsx')







# 剔除收费周期结束日期在开始日期之前的、应收额<=0的、测试项目的应收数据
# 应收为0，其实需预警，可能项目操作错误
need_recharge <- need_recharge %>% 
  mutate(cost_startdate = as_date(cost_startdate) ,
         cost_enddate = as_date(cost_enddate)) %>% 
  filter(cost_enddate >= cost_startdate , accrued_amount > 0 , project_name != '测试项目')

print(now())


# ---------- 实收表（行，读约分钟）
print(now())
income <- sqlQuery(con_sql , glue("select pk_house , cost_startdate , cost_enddate ,
                                   pk_chargebills , bill_date , real_amount
                                   FROM actual_charge_table"))
print(now())

income_stat <- income %>% 
  group_by(pk_house , pk_chargebills) %>% 
  summarise(real_amount = sum(real_amount)) %>% 
  ungroup()

print(now())


# ---------- 减免表（行，读约分钟）
# （若有物业费充值活动，优惠的金额会在减免表出现）
print(now())
relief <- sqlQuery(con_sql , glue("select pk_house , cost_startdate , cost_enddate ,
                                   pk_chargebills , adjust_amount
                                   FROM relief_cost_table"))
print(now())

relief_stat <- relief %>% 
  group_by(pk_house , pk_chargebills) %>% 
  summarise(adjust_amount = sum(adjust_amount)) %>% 
  ungroup()

print(now())


# ---------- 冲抵表（行，读约分钟）
print(now())
offset <- sqlQuery(con_sql , glue("select pk_house , cost_startdate , cost_enddate ,
                                   bill_date , match_amount
                                   FROM advance_cost_table"))
print(now())

offset_stat <- offset %>% 
  group_by(pk_house , cost_startdate , cost_enddate) %>% 
  summarise(match_amount = sum(match_amount)) %>% 
  ungroup() %>% 
  mutate(cost_startdate = as_date(cost_startdate) ,
         cost_enddate = as_date(cost_enddate))

print(now())





# 需fix情况
# 1、显示为跨月，但实际未跨月。如：pk_house 009D87217900326B8516
# 2、显示跨月，确实跨月。 如：pk_house 009CD9C5630063596C5B
# 3、显示未跨月，但该月拆为了两个区间段
# 4、显示不跨月，但某月有两条数据，其中一条为上月少收的，项目操作到本月，本月又生成一条。如：pk_house 0093D156E700E35FA7DD
need_recharge_fix <- need_recharge %>% 
  mutate(cost_startdate = as_date(cost_startdate) ,
         cost_enddate = as_date(cost_enddate)) %>% 
  filter(cost_enddate >= cost_startdate , accrued_amount > 0 , project_name != '测试项目') %>% 
  group_by(pk_house) %>% 
  mutate(next_coststart = lead(cost_startdate , 1 , order_by = cost_startdate) ,
         last_costend = lag(cost_enddate , 1 , order_by = cost_startdate)) %>% 
  ungroup() %>% 
  mutate(is_across_month = if_else(year(cost_startdate) == year(cost_enddate) &
                                     month(cost_startdate) == month(cost_enddate) , 'y' , 'n') ,#判断该笔是否跨月
         # 若一笔缴费有跨月，进行如下判断；没有跨月暂不判断
  ) 



# 需再判断
mutate(cost_startdate = case_when(cost_startdate == last_costend ~ cost_startdate + 1 ,
                                  TRUE ~ cost_startdate) ,
       cost_enddate = case_when(is.na(next_coststart) ~ cost_enddate ,
                                next_coststart == cost_enddate ~ cost_enddate - 1 , 
                                next_coststart < cost_enddate ~ next_coststart -1 , 
                                TRUE ~ cost_enddate) ,
       is_value = if_else(year(cost_startdate) == year(cost_enddate) &
                            month(cost_startdate) == month(cost_enddate) , 'y' , 'n'))

print(now())

unvalue <- need_recharge_fix %>% 
  filter(is_value == 'n')

# cs <- need_recharge_fix %>% 
#   filter(cost_startdate <= '2015-01-01')

# cs <- need_recharge %>% 
#   distinct(project_name)



SELECT chargeable.pk_house , house_name , 
chargeable.cost_startdate , chargeable.cost_enddate , 
accrued_amount , real_amount , adjust_amount , match_amount ,
CAST(ISNULL(accrued_amount , 0) AS DECIMAL(18 , 2)) - 
  CAST(ISNULL(real_amount , 0) AS DECIMAL(18 , 2)) - 
  CAST(ISNULL(adjust_amount , 0) AS DECIMAL(18 , 2)) - 
  CAST(ISNULL(match_amount , 0) AS DECIMAL(18 , 2)) AS owe ,
bill_time , chargeable.pk_chargebills ,
CLIENT_NAME , CELLPHONE , FLOOR_NAME , UNIT_NAME , 
pk_build , build_name , pk_project , project_name , 
xywy_project6 , xywy_project4 , xywy_project3 , xywy_project2 , xywy_project1
FROM
(SELECT -- top 2000 
  xywy_project1 , xywy_project2 , xywy_project3 , xywy_project4 , xywy_project6 ,
  pk_project , project_name , pk_build , build_name , pk_house , house_name , 
  cost_startdate , cost_enddate , accrued_amount , pk_chargebills
  FROM chargeable_table
  WHERE PK_HOUSE in ('00940A47C70018C98B07' , '0045B730FA00143A3D1D')
  AND COST_ENDDATE >= COST_STARTDATE
  AND accrued_amount > 0
) chargeable /*应收，每个应收id只会有一条记录*/
  LEFT JOIN
(SELECT pk_chargebills , sum(CAST(real_amount AS DECIMAL(18 , 2))) real_amount , 
  stuff(( SELECT ',' + CONVERT(nvarchar(50) , t1.bill_date , 20) FROM actual_charge_table t1
          WHERE t1.pk_chargebills = actual_charge_table.pk_chargebills 
          FOR XML path('')) , 1 , 1 , '') as bill_time 
  FROM actual_charge_table
  WHERE PK_HOUSE in ('00940A47C70018C98B07' , '0045B730FA00143A3D1D')
  GROUP BY pk_chargebills) income /*实收，一个应收id可能会有多条实收记录*/
  ON chargeable.pk_chargebills = income.pk_chargebills
LEFT JOIN
(SELECT pk_chargebills , adjust_amount
  FROM relief_cost_table) relief /*减免，一个应收id可能会有多条减免记录*/
  ON chargeable.pk_chargebills = relief.pk_chargebills
LEFT JOIN
(SELECT pk_house , match_amount , cost_startdate , cost_enddate
  FROM advance_cost_table) offset /*冲抵，同一户在同月可能会有多条冲抵记录*/
  ON chargeable.pk_house = offset.pk_house
AND chargeable.cost_startdate = offset.cost_startdate 
AND chargeable.cost_enddate = offset.cost_enddate
LEFT JOIN
(SELECT PK_HOUSE , UNIT_NAME , FLOOR_NAME , CLIENT_NAME , CELLPHONE
  FROM dim_owner_basic_info) info /*业主信息*/
  ON chargeable.pk_house = info.PK_HOUSE -- ) Z

ORDER BY PK_HOUSE , COST_STARTDATE
;



