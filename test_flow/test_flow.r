source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')


# table <- 'test_dim_owner_basic_info'

# 查询数据
# ROWNUM < 10 and 
test <- dbGetQuery(net_orc , glue("SELECT house.pk_project pk_project , 
project_name , house.pk_build pk_build , build_name , 
house.pk_unit pk_unit , unit_name , house.pk_floor pk_floor , 
floor_name , house.pk_house pk_house , house_name , 
houseclient.pk_client pk_client , client_name , 
DECODE(SEX , 'NULL' , '' , '#VALUE!' , '' , SEX) sex , 
DECODE(CELLPHONE , 'NULL' , '' , CELLPHONE) cellphone , 
DECODE(PHONE , 'NULL' , '' , PHONE) phone , 
DECODE(IDCARD , 'NULL' , '' , IDCARD) idcard , 
pk_userlevel , userlevel_value , 
pk_creditline , creditline_value , 
house.house_type house_type , house_type_name , 
house.house_state house_state, 
NVL(house_state_name , house.house_state) house_state_name , 
build_area , SYSDATE AS d_t
FROM
(SELECT pk_house , pk_project , pk_build , pk_unit , pk_floor , 
  house_name , house_type , house_state , build_area
  FROM LS_XYWY.RES_HOUSE
  where pk_project in ('00BC7DF66500F079B0EC') 
  AND dr = 0 ) house 
LEFT JOIN
(SELECT pk_house , pk_client
  FROM LS_XYWY.RES_HOUSECLIENT
  WHERE client_type = 0 and dr = 0) houseclient
on house.pk_house = houseclient.pk_house
LEFT JOIN
(SELECT pk_client , client_name , sex , cellphone , phone , idcard , pk_userlevel , pk_creditline
  FROM LS_XYWY.RES_CLIENT
  WHERE dr = 0) client
on houseclient.pk_client = client.pk_client
LEFT JOIN
(SELECT pk_project , project_name
  FROM LS_XYWY.RES_PROJECT
  WHERE dr = 0) project
on house.pk_project = project.pk_project
LEFT JOIN
(SELECT pk_build , build_name
  FROM LS_XYWY.RES_BUILD
  WHERE dr = 0) build
on house.pk_build = build.pk_build
LEFT JOIN
(SELECT pk_unit , unit_name
  FROM LS_XYWY.RES_UNIT
  WHERE dr = 0) unit
on house.pk_unit = unit.pk_unit
LEFT JOIN
(SELECT pk_floor , floor_name
  FROM LS_XYWY.RES_FLOOR
  WHERE dr = 0) floor
on house.pk_floor = floor.pk_floor
LEFT JOIN
(SELECT PK_DATADICTIONARY , NAME as house_type_name
  FROM LS_XYWY.LSBD_DATADICTIONARY
  WHERE dr = 0) map_table1
on house.house_type = map_table1.PK_DATADICTIONARY
LEFT JOIN
(SELECT PK_DATADICTIONARY , NAME as house_state_name
  FROM LS_XYWY.LSBD_DATADICTIONARY
  WHERE dr = 0) map_table2
on house.house_state = map_table2.PK_DATADICTIONARY
LEFT JOIN
(SELECT PK_DATADICTIONARY , NAME as creditline_value
  FROM LS_XYWY.LSBD_DATADICTIONARY
  WHERE dr = 0) map_table3
on client.PK_CREDITLINE = map_table3.PK_DATADICTIONARY
LEFT JOIN
(SELECT PK_DATADICTIONARY , name as userlevel_value
  FROM LS_XYWY.LSBD_DATADICTIONARY
  WHERE dr = 0) map_table4
on client.PK_USERLEVEL = map_table4.PK_DATADICTIONARY")) %>% 
  mutate(D_T = as_datetime(D_T))


# 将列名统一改为小写
names(test) <- tolower(names(test))


# 写入sql server
# 使用此函数写入时，需注意在sql server中建好的表的字段类型，一定要适用数据，否则报错
# 使用此函数时，一定要保证数据库中列同此表输出列一致，否则报错
# 此处设定append=TRUE，若为false，此函数会自行在数据库建表，类型修改麻烦，且若库中已有同名表会报错，因此建议设定append=TRUE
# 若为增量，先执行清空表的操作，再执行入库
sqlClear(net_sql, 'test_dim_owner_basic_info')
sqlSave(net_sql, test, tablename = "test_dim_owner_basic_info", append=TRUE , rownames=FALSE , fast = FALSE)

