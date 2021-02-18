# 本地使用
# source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')
# 调度使用
source('/root/data/env_centos.r' , encoding = 'utf8')
author <- c('huruiyi')
# 命名规则: 表应归属库_表类型_归属部门_业务大类_表详细归类
table <- 'mid_dim_project_hierarchy'      # 项目层级表


# 直接从sql server数据库内拉取数据(设置了填报，业务更新库内即会更新)
# 把id删除，mysql可设置自增id
project_data <- sqlQuery(con_sqls , glue("select * from mid_dim_project_hierarchy")) %>% 
  select(-id)


# MySQL入库
conn <- dbConnect(con_mid)

# 先清空表内数据，再插入数据。因直接使用覆盖数据，会重新建表，新建表的字段类型不符合规则
delete <- dbGetQuery(conn , glue("truncate table {table};"))
dbWriteTable(conn , table , project_data , append = TRUE , row.names = FALSE)
print(paste0('MySQL ETL project hierarchy success: ' , now()))
