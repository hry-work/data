source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_dim_project_kpi_provide'      # 项目层级表

kpi_project <- read.xlsx('C:/Users/Administrator/data/mid/dim/mid_dim_project_kpi_provide.xlsx' , detectDates = TRUE) %>% 
  mutate(id = row_number(),
         d_t = now()) %>% 
  select(id , kpi_project_name , project_name , provide_name , source , nature ,
         contract_start , contract_end , is_delivery , first_delivery_date ,
         expected_delivery_date , build_area , delivered_area , undelivery_area,
         is_oc_project , d_t)


# 替换na为字符型的na，否则入库oracle时报错
kpi_project[is.na(kpi_project)] <- NA_character_


# sqlserver入库
sqlClear(con_sqls, table)
sqlSave(con_sqls , kpi_project , tablename = table ,
        append = T , rownames = FALSE , fast = FALSE)

print(paste0('SQL Server ETL project kpi_project success: ' , now()))


# oracle入库，注意表名一定要大写
dbWriteTable(con_orc , toupper(table) , kpi_project , overwrite=TRUE)

print(paste0('ORACLE ETL project kpi_project success: ' , now()))

