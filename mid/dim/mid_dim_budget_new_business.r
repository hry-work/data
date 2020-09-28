source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_dim_budget_new_business'      # 多经预算表

# 获取项目id
project_id <- dbGetQuery(con_orc , glue("select pk_project , project_name
                                         from res_project
                                         where dr = 0 "))

names(project_id) <- tolower(names(project_id))


budget_data <- read.xlsx('..\\data\\mid\\dim\\多经预算.xlsx' , detectDates = TRUE) %>% 
  left_join(project_id , by = 'project_name') %>% 
  mutate(d_t = now() ,
         id = row_number() ,
         is_project = if_else(is.na(pk_project) , 0 , 1) ,
         sale_water = round(sale_water , 2) ,
         property_agency = round(property_agency , 2) ,
         decorate_manage = round(decorate_manage , 2) ,
         sale_goods = round(sale_goods , 2) ,
         other1 = round(other1 , 2) ,
         other2 = round(other2 , 2) ,
         other3 = round(other3 , 2) ,
         all_budget = round(all_budget , 2)) %>% 
  select(id , pk_project , project_name , is_project , month , month_start , budget_type , tax , 
         sale_water , sale_goods , property_agency , decorate_manage , 
         other1 , other2 , other3 , all_budget , d_t)

# cs <- budget_data %>%
#   filter(is.na(pk_project)) %>%
#   distinct(pk_project , project_name)

# 入库
sqlClear(con_sql, table)
sqlSave(con_sql , budget_data , tablename = table ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL new business budget success: ' , now()))
