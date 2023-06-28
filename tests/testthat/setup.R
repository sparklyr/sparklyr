# Initializing
ul <- ifelse(using_livy(), using_livy_version(), "No")
ua <- ifelse(using_arrow(), using_arrow_version(), "No")

cat("\n----- sparklyr test setup ----")
cat("\nSpark:", testthat_spark_env_version())
cat("\nLivy:", ul)
cat("\nArrow:", ua)
if(using_method()) {
  cat("\n-----------")
  cat("\nMaster:", using_master_get())
  cat("\nMethod:", using_method_get())
}
if(using_arrow()) cat("\n  |---", as.character(packageVersion("arrow")))
cat("\n------------------------------\n")

cat("\n--- Creating Spark session ---\n")
sc <- testthat_spark_connection()
cat("------------------------------\n\n")

## Disconnects all at the end
withr::defer(spark_disconnect_all(), teardown_env())
