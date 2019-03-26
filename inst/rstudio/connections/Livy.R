library(sparklyr)
sc <- spark_connect(master = "${1:Livy URL=http$colon$//server/livy/}",
                    version = "${2:Version=2.4.0}",
                    method = "livy", config = livy_config(
                      username = "${3:User}",
                      password = rstudioapi::askForPassword("Livy password:")))
