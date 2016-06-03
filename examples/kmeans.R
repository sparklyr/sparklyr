library(rspark)
library(dplyr)

sc <- spark_connect(version = "2.0.0-preview")
ctx <- spark_context(sc)
sql <- rspark:::spark_api_create_sql_context(sc)
db <- src_spark(sc)

names(iris) <- gsub("[^a-zA-Z0-9]", "_", names(iris))
copy_to(db, iris, "iris")
iris_tbl <- tbl(db, "iris")

kmm <- iris_tbl %>%
  select(Sepal_Width, Sepal_Length) %>%
  spark_mllib_kmeans(3)

rspark:::spark_inspect(kmm)
spark_invoke(kmm, "k")
