library(rspark)
library(dplyr)

sc <- spark_connect(version = "2.0.0-preview")
ctx <- spark_context(sc)
db <- src_spark(sc)

spark_invoke(ctx, "toString")

# TODO: It seems like '.'s in names causes some internal
# errors within Spark.
names(iris) <- gsub("[^a-zA-Z0-9]", "_", names(iris))
copy_to(db, iris, "iris")
iris_tbl <- tbl(db, "iris")

sqlResult <- spark_invoke(
  rspark:::spark_sql_or_hive(db$con@api),
  "sql",
  "SELECT * FROM iris"
)

rdd <- spark_invoke(sqlResult, "rdd")
