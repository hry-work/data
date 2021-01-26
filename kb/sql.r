
# ---------- 适用于sql server
# 通过sqlFetch()函数访问EMP表
data1 <- sqlFetch(myconn,"EMP")   

# 通过sqlQuery()函数访问EMP表
count = sqlQuery(net_sql,"select count(1) from xywy_project")

# 断开链接
close(net_sql)



# ---------- 适用于oracle（使用rjdbc连接，因此需使用rjdbc函数）
# 通过dbGetQuery访问chargeable_table表
need_recharge <- dbGetQuery(net_orc , glue("select * FROM chargeable_table where rownum < 20"))