source('env.r' , encoding = 'utf8')

table <- 'test_dim_owner_basic_info'

# 查询数据
test <- dbGetQuery(net_orc , glue("SELECT house.pk_project , project_name , house.pk_build , build_name , 
house.pk_unit , unit_name , house.pk_floor , floor_name , 
house.pk_house , house_name , houseclient.pk_client , client_name , 
DECODE(SEX , 'NULL' , '' , '#VALUE!' , '' , SEX) sex , 
DECODE(CELLPHONE , 'NULL' , '' , CELLPHONE) cellphone , 
DECODE(PHONE , 'NULL' , '' , PHONE) phone , 
DECODE(IDCARD , 'NULL' , '' , IDCARD) idcard , 
pk_userlevel , userlevel_value , pk_creditline , creditline_value , 
house.house_type , house_type_name , house.house_state , 
NVL(HOUSE_STATE_NAME , house.HOUSE_STATE) house_state_name , 
build_area , SYSDATE AS d_t
FROM
(SELECT PK_HOUSE , PK_PROJECT , PK_BUILD , PK_UNIT , PK_FLOOR , 
  HOUSE_NAME , HOUSE_TYPE , HOUSE_STATE , BUILD_AREA
  FROM LS_XYWY.RES_HOUSE
  where PK_BUILD in ('00BC7EB62500BAFF96DB') 
  AND ROWNUM < 10 and dr = 0 ) house 
LEFT JOIN
(SELECT PK_HOUSE , PK_CLIENT
  FROM LS_XYWY.RES_HOUSECLIENT
  WHERE client_type = 0 and dr = 0) houseclient
on house.PK_HOUSE = houseclient.PK_HOUSE
LEFT JOIN
(SELECT PK_CLIENT , CLIENT_NAME , SEX , CELLPHONE , PHONE , IDCARD , PK_USERLEVEL , PK_CREDITLINE
  FROM LS_XYWY.RES_CLIENT
  WHERE dr = 0) client
on houseclient.PK_CLIENT = client.PK_CLIENT
LEFT JOIN
(SELECT PK_PROJECT , PROJECT_NAME
  FROM LS_XYWY.RES_PROJECT
  WHERE dr = 0) project
on house.PK_PROJECT = project.PK_PROJECT
LEFT JOIN
(SELECT PK_BUILD , BUILD_NAME
  FROM LS_XYWY.RES_BUILD
  WHERE dr = 0) build
on house.PK_BUILD = build.PK_BUILD
LEFT JOIN
(SELECT PK_UNIT , UNIT_NAME
  FROM LS_XYWY.RES_UNIT
  WHERE dr = 0) unit
on house.PK_UNIT = unit.PK_UNIT
LEFT JOIN
(SELECT PK_FLOOR , FLOOR_NAME
  FROM LS_XYWY.RES_FLOOR
  WHERE dr = 0) floor
on house.PK_FLOOR = floor.PK_FLOOR
LEFT JOIN
(SELECT PK_DATADICTIONARY , NAME as HOUSE_TYPE_NAME
  FROM LS_XYWY.LSBD_DATADICTIONARY
  WHERE dr = 0) map_table1
on house.HOUSE_TYPE = map_table1.PK_DATADICTIONARY
LEFT JOIN
(SELECT PK_DATADICTIONARY , NAME as HOUSE_STATE_NAME
  FROM LS_XYWY.LSBD_DATADICTIONARY
  WHERE dr = 0) map_table2
on house.HOUSE_STATE = map_table2.PK_DATADICTIONARY
LEFT JOIN
(SELECT PK_DATADICTIONARY , NAME as CREDITLINE_VALUE
  FROM LS_XYWY.LSBD_DATADICTIONARY
  WHERE dr = 0) map_table3
on client.PK_CREDITLINE = map_table3.PK_DATADICTIONARY
LEFT JOIN
(SELECT PK_DATADICTIONARY , name as USERLEVEL_VALUE
  FROM LS_XYWY.LSBD_DATADICTIONARY
  WHERE dr = 0) map_table4
on client.PK_USERLEVEL = map_table4.PK_DATADICTIONARY"))


# 写入sql server

test <- dbReadTable(net_sql , table)


test <- dbWriteTable(net_sql, table)

dbWriteTable(net_sql, table , test)
