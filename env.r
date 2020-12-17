
# 释放内存
gc()

options(java.parameters = "-Xmx100g")
# 运行内存扩大
memory.limit(102400)

# install.packages('utf8')
# install.packages('rJava')
# install.packages('glue')
# install.packages('tidyr')
# install.packages('dplyr')
# install.packages('lubridate')
# install.packages('plyr')
# install.packages('stringr')
# install.packages('readr')
# install.packages('readxl')
# install.packages('openxlsx')
# install.packages('zoo')
# install.packages('car')
# install.packages('RODBC')
# install.packages('RSQLite')
# install.packages('ggplot2')
# install.packages(c('vctrs' , 'httr' , 'rvest'))
# install.packages('xml2')
# install.packages("RJDBC")
# install.packages("DBI")
# install.packages("carData")
# install.packages("xts")
# install.packages("sqldf")
# install.packages("class")
# install.packages("RMySQL")


# 加载包
library('utf8')
library('rJava')
library('DBI')
library('RODBC')
library('RJDBC')
library('sqldf')
library('RSQLite')
library('RMySQL')
library('glue')
library('tidyr')
library('plyr')
library('dplyr')
library('lubridate')
library('stringr')
library('readr')
library('readxl')
library('openxlsx')
library('class')
library('zoo')
library('carData')
library('car')
library('ggplot2')
library('vctrs')
library('httr')
library('xml2')
library('rvest')
library('xts')


# ---------- 关联数据库

# ----- 关联oracle（使用rjdbc的方式，rodbc及roracle此电脑有坑）
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


# ----- 关联sql server
con_sql <- odbcConnect('orcl', uid='sa' , pwd='xywy2020.')


# ----- 关联mysql
con_mysql <- dbConnect(MySQL(), dbname = "clound", username="xywy", password="xywy2020.", host="192.168.128.133", port=3306)


# 日期
today <- as_date(today())
day <- as_date(today - 1)

print('source env.r success')