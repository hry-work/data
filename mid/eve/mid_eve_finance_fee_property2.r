source('env.r' , encoding = 'utf8')

author <- c('huruiyi')
table <- 'mid_eve_finance_fee_property'      # 物业费事件表（已修正的）
# 命名规则：表应归属库_表类型_归属部门_业务大类_表详细归类


# -------------------- 准备基础数据 --------------------

# ---------- 物业费code
property_code <- dbGetQuery(net_orc , glue("select distinct pk_projectid , projectcode , projectname
                                            from wy_bd_fmproject 
                                            where projectcode in ('001','002','003','004')"))

# 项目基础数据
basic_info <- sqlQuery(net_sql , "select pk_house , house_name , floor_name , unit_name , build_name , project_name
                                  from dim_owner_basic_info")

# 项目归属区域
belong <- dbGetQuery(net_orc , glue("select porject1 , porject4 , porject6 from xywy_project"))






# ---------- 应收明细表（近600万行，读约8分钟）
# 一个pk_chargebills只会有一条记录
print(now())
chargebills <- dbGetQuery(net_orc , glue("select pk_chargebills , pk_project , pk_build , 
                                          pk_unit , pk_floor , pk_house , pk_projectid , 
                                          cost_date , cost_startdate , cost_enddate , 
                                          accrued_amount , dr
                                          from wy_bill_chargebills"))
print(now())





select mt.porject1,mt.porject2,mt.porject3,mt.porject4,mt.porject6,wbgd.real_amount,resp.pk_project,resp.project_name,resh.house_name,wbg.bill_date,
resb.pk_build,resb.build_name,wbc.pk_chargebills,wbc.cost_startdate,wbc.cost_enddate,resh.pk_house,wbgt.code from wy_bill_chargebills wbc
left join wy_bill_gathering_d wbgd on wbgd.source=wbc.pk_chargebills and wbgd.dr=0
left join wy_bill_gathering wbg on wbg.pk_gathering=wbgd.pk_gathering and wbg.dr=0
left join wy_bd_gatheringtype wbgt on wbgd.pk_gathering_type=wbgt.pk_gatheringtype and wbgt.dr=0
left join wy_bd_fmproject wbf on wbc.pk_projectid = wbf.pk_projectid
left join res_project resp on resp.pk_project=wbc.pk_project and resp.dr=0
left join Res_build resb on resb.pk_build=wbc.pk_build and resb.dr=0
left join res_house resh on resh.pk_house=wbc.pk_house and resh.dr=0
left join xywy_project mt on mt.porject6=resp.project_name
where wbc.dr=0 and wbf.projectcode in ('001','002','003','004')

--and mt.porject6='长沙鑫苑木莲世家'
--and resh.house_name='3-2-1405'
and wbc.cost_startdate between '1990-01-01' and '2020-12-31'
and wbc.cost_enddate between '1990-01-01' and '2020-12-31'
and (to_char(wbg.bill_date, 'yyyy-MM-dd') >= '1990-01-01 00:00:00' and  to_char(wbg.bill_date, 'yyyy-MM-dd')<='2020-12-31')		




# 剔除收费周期结束日期在开始日期之前的、应收额<=0的、测试项目的应收数据
# 应收为0，其实需预警，可能项目操作错误
need_recharge <- need_recharge %>% 
  mutate(cost_startdate = as_date(cost_startdate) ,
         cost_enddate = as_date(cost_enddate)) %>% 
  filter(cost_enddate >= cost_startdate , accrued_amount > 0 , project_name != '测试项目')

print(now())


# ---------- 实收表（行，读约分钟）
print(now())
income <- sqlQuery(net_sql , glue("select pk_house , cost_startdate , cost_enddate ,
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
relief <- sqlQuery(net_sql , glue("select pk_house , cost_startdate , cost_enddate ,
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
offset <- sqlQuery(net_sql , glue("select pk_house , cost_startdate , cost_enddate ,
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



# 断开sql server连接
close(net_sql)