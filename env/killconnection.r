#数据库连接删除函数，每个任务之前最好先清理所有的连接，调用此函数就可以
killDbConnections <- function () {
  all_cons <- dbListConnections(MySQL())
  print(all_cons)
  for(con in all_cons){
    dbDisconnect(con)
  }
  print(paste(length(all_cons), " connections killed."))
}

killDbConnections()