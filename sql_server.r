
# 通过sqlFetch()函数访问EMP表
data1 <- sqlFetch(myconn,"EMP")   

# 通过sqlQuery()函数访问EMP表
count = sqlQuery(net_sql,"select count(1) from xywy_project")

# 断开链接
close(net_sql)