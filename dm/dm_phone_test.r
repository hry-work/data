print('phone flow start')

# 本地使用
# source('C:/Users/Administrator/data/env.r' , encoding = 'utf8')

# 调度使用
source('/root/data/env_centos.r' , encoding = 'utf8')

author <- c('huruiyi')

print(author)

# 测试从sql server取数
test_write <- sqlQuery(con_sqls , "select top 1 pk_project , pk_build , pk_unit , 
                       pk_floor , pk_house , house_name , build_area
                       from mid_dim_owner_basic_info") %>% 
  mutate(d_t = now())


# 连接mysql，写入数据

# 连接mysql dm层
# conn <- dbConnect(con_dm)

# test_2 <- dbGetQuery(conn , "select * from test")

# 写入
dbWriteTable(con_dm , 'test' , test_write , append = T , row.names = F)
