# 本地使用
source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_dim_project_hierarchy'      # 项目层级表

# 直接从sql server数据库内拉取数据(设置了填报，业务更新库内即会更新)

project_data <- read_excel('..\\data\\mid\\dim\\mid_dim_project_hierarchy.xlsx') %>% 
  mutate(operate_time = if_else(is.na(operate_time) , as_datetime(now()) , as_datetime(operate_time)) ,
         operate_date = if_else(is.na(operate_date) , as_date(operate_time) , as_date(operate_date)) ,
         operator = as.character(operator)) %>% 
  select(project_name , project4 , project3 , project2 , project1 , belong , 
         province , city , county , wy_cycle , csd_project , 
         fdproject_property_h , fdproject_property_a , fdproject_car , 
         fdproject_business , operate_date , operate_time , operator)


# sqlserver入库
sqlClear(con_sqls, table)
sqlSave(con_sqls , project_data , tablename = table ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('SQL Server ETL project hierarchy success: ' , now()))


# oracle入库，注意表名一定要大写
# 先清除数据
# dbSendUpdate(con_orc , "delete from MID_DIM_PROJECT_HIERARCHY")
# 写入数据（若overwrite为true，则会新建表，字段类型不符合要求），一直报错，暂还使用老方式让其自动覆盖表
# dbWriteTable(con_orc , toupper(table) , project_data , overwrite = FALSE, append=TRUE)

dbWriteTable(con_orc , toupper(table) , project_data , overwrite=TRUE)

print(paste0('ORACLE ETL project hierarchy success: ' , now()))


