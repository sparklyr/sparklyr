library(sparklyr)

sc <- spark_connect(master = "local")

sdf_len(sc, 10) %>% spark_write_csv("batch.csv")

spark_disconnect(sc)
