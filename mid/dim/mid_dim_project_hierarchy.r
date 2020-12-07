source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_dim_project_hierarchy'      # 项目层级表

project_data <- read.xlsx('..\\data\\mid\\dim\\项目层级表.xlsx' , detectDates = TRUE) %>% 
  mutate(operate_time = if_else(is.na(operator) , as.character(now()) , as.character(paste0('2020/11/16 ', '18:02:50'))) ,
         operate_date = as_date(operate_time) ,
         operator = as.character(operator)) %>% 
  select(id , project_name , project4 , project3 , project2 , project1 , belong , wy_cycle ,
         phone_project , csd_project , fdproject_property_h , fdproject_property_a , 
         fdproject_car , fdproject_business , operate_date , operate_time , operator)



# sqlserver入库
sqlClear(con_sql, table)
sqlSave(con_sql , project_data , tablename = table ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL project hierarchy success: ' , now()))



# oracle入库，注意表名一定要大写
# newData <- project_data[,utfCol:=iconv(gbkCol,from="gbk",to="utf-8")]
dbWriteTable(con_orc , 'MID_DIM_PROJECT_HIERARCHY' , project_data , overwrite=TRUE)


print(paste0('ETL project hierarchy success: ' , now()))