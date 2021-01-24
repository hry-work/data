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

# options(java.parameters = "-Xmx100g")
# # 运行内存扩大
# memory.limit(102400)

# 加载包
source('/root/data/env/packages.r', encoding = "utf-8")

# 每次连库前先关闭数据库连接，否则连接过多时会报错
source('/root/data/env/killconnection.r', encoding = "utf-8")

# 关联数据库
source('/root/data/env/connect.r', encoding = "utf-8")

# 函数
source('/root/data/env/function.r', encoding = "utf-8")

# 参数
source('/root/data/env/param.r', encoding = "utf-8")

# 公共函数
source('/root/data/env/function_pub.r', encoding = "utf-8")

# 日期
source('/root/data/env/date.r', encoding = "utf-8")


print('source env success')