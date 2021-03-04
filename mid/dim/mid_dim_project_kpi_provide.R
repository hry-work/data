source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

author <- c('huruiyi')

# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_dim_project_kpi_provide'      # 计划考核提供基础资源表

# 基础资源
basic <- dbGetQuery(con_orc , glue("select distinct pk_project , project_name
                                   from res_project
                                   where dr = 0"))
names(basic) <- tolower(names(basic))

kpi_project <- read.xlsx('C:/Users/Administrator/data/mid/dim/mid_dim_project_kpi_provide.xlsx' , detectDates = TRUE) %>% 
  left_join(basic) %>% 
  mutate(pk_project = if_else(is.na(pk_project) , kpi_project , pk_project),
         dr = 0 ,
         d_t = now()) %>% 
  select(kpi_project , project_name , pk_project , province , city , county , delivery_cnt , 
         is_delivery , source , nature , contract_signed , contract_start , contract_end , 
         delivery_date ,build_area , delivered_area , yetai_classify , yetai , project_address , 
         our , opposite , contract_project , takeover_year , dr , operator , d_t)

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

