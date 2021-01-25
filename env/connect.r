# --------------- 关联mysql ---------------

# 乐软考勤
con_punch <- dbConnect(MySQL(), dbname = "clound", username="xywy", password="xywy2020.", host="192.168.128.133", port=3306)
print('connect con_punch success')

# 鑫苑物业dm展示层
con_dm <- dbConnect(MySQL(), dbname = "dm", username="root", password="XYwy2020.", host="192.168.128.234", port=3306)
print('connect con_dm success')

# 鑫苑物业mid中间层
con_mid <- dbConnect(MySQL(), dbname = "mid", username="root", password="XYwy2020.", host="192.168.128.234", port=3306)
print('connect con_dm success')


# --------------- 关联sql server ---------------
con_sqls <- odbcConnect('con_sqls', uid='sa' , pwd='xywy2020.')
print('sqlserver link success')


# --------------- 关联oracle ---------------
# # oracle当前暂不需
# # ----- 关联oracle（使用rjdbc的方式，rodbc及roracle此电脑有坑）
# if(file.exists("G:/") == TRUE) {
#   # --- 此版适用远程
#   drv <-JDBC("oracle.jdbc.driver.OracleDriver",
#              "G:/app/oracle/product/11.2.0/client_1/jdbc/lib/ojdbc6_g.jar",
#              identifier.quote="\"")
# } else if(file.exists("D:/") == TRUE) {
#   # --- 此版适用本地
#   drv <-JDBC("oracle.jdbc.driver.OracleDriver",
#              "D:/u01/app/oracle/product/11.2.0/client_1/jdbc/lib/ojdbc6_g.jar",
#              identifier.quote="\"")
# }
# 
# print('jdbc input success')
# 
# con_orc <-dbConnect(drv,"jdbc:oracle:thin:@192.168.128.215:1521/ORCL","ls_xywy","ls_xywy")
# 
# print('oracle link success')