source('/root/data/env_centos.r' , encoding = 'utf8')

while (hm(str_c(hour(Sys.time()),minute(Sys.time()), sep=':')) < hm(NOOP_START)) {
	print(glue('wait to {NOOP_START} am to start'))
	sleep(60*1)
}