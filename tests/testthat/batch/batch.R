library(sparklyr)

sc <- spark_connect(master = "local")

message(paste("batch.R: SPARK_HOME is", Sys.getenv("SPARK_HOME")))

sdf_len(sc, 10) %>% spark_write_csv("batch.csv")

spark_disconnect(sc)
