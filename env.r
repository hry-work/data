# *********************************
#Sys.setlocale(category = "LC_ALL", locale="UTF-8")
# 全局设置时区，辅导业务库mongodb使用的是UTC时间
# 局部可根据具体时区单独设置
Sys.setenv(TZ="PRC")

# 取消科学计数法显示数字
options(scipen = 200)
options(encoding = 'utf-8')
options(lubridate.week.start = 1)
options(stringsAsFactors = FALSE)


# 释放内存
gc()

options(java.parameters = "-Xmx100g")
# 运行内存扩大
memory.limit(102400)


# 加载包
source('C:/Users/Administrator/data/env/packages.r', encoding = "utf-8")

# 每次连库前先关闭数据库连接，否则连接过多时会报错
source('C:/Users/Administrator/data/env/killconnection.r', encoding = "utf-8")


# ---------- 关联数据库---------------

# ----- 关联oracle（使用rjdbc的方式，rodbc及roracle此电脑有坑）---------------
if(file.exists("G:/") == TRUE) {
  # --- 此版适用远程
  drv <-JDBC("oracle.jdbc.driver.OracleDriver",
             "G:/app/oracle/product/11.2.0/client_1/jdbc/lib/ojdbc6_g.jar",
             identifier.quote="\"")
} else if(file.exists("D:/") == TRUE) {
  # --- 此版适用本地
  drv <-JDBC("oracle.jdbc.driver.OracleDriver",
             "D:/u01/app/oracle/product/11.2.0/client_1/jdbc/lib/ojdbc6_g.jar",
             identifier.quote="\"")
}


con_orc <-dbConnect(drv,"jdbc:oracle:thin:@192.168.128.215:1521/ORCL","ls_xywy","ls_xywy")


# ----- 关联sql server---------------
con_sqls <- odbcConnect('orcl', uid='sa' , pwd='xywy2020.')


# ----- 关联mysql---------------
# 乐软考勤
con_punch <- dbConnect(MySQL(), dbname = "clound", username="xywy", password="xywy2020.", host="192.168.128.133", port=3306)
# dm展示层
con_dm <- dbConnect(MySQL(), dbname = "dm", username="root", password="XYwy2020.", host="192.168.128.234", port=3306)
# 鑫苑物业mid中间层
con_mid <- dbConnect(MySQL(), dbname = "mid", username="root", password="XYwy2020.", host="192.168.128.234", port=3306)
print('connect con_dm success')


# 函数
source('C:/Users/Administrator/data/env/function.r', encoding = "utf-8")

# 参数
source('C:/Users/Administrator/data/env/param.r', encoding = "utf-8")

# 公共函数
source('C:/Users/Administrator/data/env/function_pub.r', encoding = "utf-8")

# 日期
source('C:/Users/Administrator/data/env/date.r', encoding = "utf-8")




print('source env success')