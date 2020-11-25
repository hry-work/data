source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_dim_budget_property'      # 物业费费预算表

budget_data <- read.xlsx('..\\data\\mid\\dim\\物业费预算.xlsx' , detectDates = TRUE) %>% 
  mutate(budget_amount = round(budget_amount * 10000 , 2) ,
         budget_clear = round(budget_clear * 10000 , 2) ,
         d_t = now())



# 入库
sqlClear(con_sql, table)
sqlSave(con_sql , budget_data , tablename = table ,
        append = TRUE , rownames = FALSE , fast = FALSE)

print(paste0('ETL property budget success: ' , now()))