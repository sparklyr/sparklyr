library(sparklyr)
config = livy_config(username = "${2:User}", password = "${3:Password}")
sc <- spark_connect(master = "${1:Livy URL=http$colon$//server/livy/}", method = "livy", config = config)
