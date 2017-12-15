library(sparklyr)
sc <- spark_connect(master = "${1:Livy URL=http$colon$//server/livy/}",
                    method = "livy", config = livy_config(
                      username = "${2:User}",
                      password = rstudioapi::askForPassword("Livy password:")))
