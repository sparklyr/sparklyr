library(rspark)
library(dplyr)

sc <- spark_connect(version = "2.0.0-preview")
ctx <- spark_context(sc)
sql <- rspark:::spark_api_create_sql_context(sc)
db <- src_spark(sc)

names(iris) <- gsub("[^a-zA-Z0-9]", "_", names(iris))
copy_to(db, iris, "iris")
iris_tbl <- tbl(db, "iris")

subset <- iris_tbl %>%
  select(Sepal_Width, Sepal_Length)

rdd <- rspark:::as_spark_rdd(subset)
rspark:::spark_inspect(rdd)

kmm <- spark_mllib_kmeans(subset, 3L, 20L)
spark_invoke(kmm, "clusterCenters")

spark_disconnect(sc)
