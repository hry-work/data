source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_dim_project_hierarchy'      # 项目层级表

project_data <- read.xlsx('..\\data\\mid\\dim\\项目层级表.xlsx' , detectDates = TRUE) %>% 
  mutate(operate_time = now() ,
         operate_date = as_date(operate_time) ,
         handover_date = as_date(handover_date) ,
         operator = as.character(operator)) %>% 
  select(id , project_name , project5 , project4 , project3 , project2 , project1 , 
         belong , province , city , county , project_from , handover_date ,
         is_focus , is_terminate , terminate_date , fdproject_property_a , is_detail_property_a ,
         fdproject_property_h , is_detail_property_h , fdproject_car , is_detail_car ,
         fdproject_business , is_detail_business , operate_date , operate_time , operator)



# 入库
sqlClear(con_sql, table)
sqlSave(con_sql , project_data , tablename = table ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL project hierarchy success: ' , now()))
