library(rspark)
library(dplyr)

sc <- spark_connect(version = "2.0.0-preview")
ctx <- spark_context(sc)
sql <- rspark:::spark_api_create_sql_context(sc)
db <- src_spark(sc)

names(iris) <- gsub("[^a-zA-Z0-9]", "_", names(iris))
copy_to(db, iris, "iris")
iris_tbl <- tbl(db, "iris")

kmeans_wrapper <- rspark:::spark_mllib_kmeans(sc, iris_tbl, 3)
centers <- spark_invoke(kmeans_wrapper, "fitted", "centers")
spark_collect(centers)
