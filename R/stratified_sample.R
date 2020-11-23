#' @include sdf_interface.R
NULL

sdf_stratified_sample_n <- function(x, grps, k, weight, replace, seed) {
  sc <- spark_connection(x)
  if (spark_version(sc) < "3.0.0") {
    stop("Stratified sampling is only supported with Spark 3.0 or higher")
  }

  method <- ifelse(
    replace, "sampleWithReplacement", "sampleWithoutReplacement"
  )
  schema <- x %>% spark_dataframe() %>% invoke("schema")

  invoke_static(
    sc,
    "sparklyr.StratifiedSamplingUtils",
    method,
    spark_dataframe(x),
    as.list(grps),
    weight,
    as.integer(k),
    as.integer(seed %||% Sys.time())
  ) %>%
    invoke(hive_context(sc), "createDataFrame", ., schema) %>%
    sdf_register()
}

sdf_stratified_sample_frac <- function(x, grps, frac, weight, replace, seed) {
  sc <- spark_connection(x)
  if (spark_version(sc) < "3.0.0") {
    stop("Stratified sampling is only supported with Spark 3.0 or higher")
  }

  method <- ifelse(
    replace, "sampleFracWithReplacement", "sampleFracWithoutReplacement"
  )
  schema <- x %>% spark_dataframe() %>% invoke("schema")

  invoke_static(
    sc,
    "sparklyr.StratifiedSamplingUtils",
    method,
    spark_dataframe(x),
    as.list(grps),
    weight,
    frac,
    as.integer(seed %||% Sys.time())
  ) %>%
    invoke(hive_context(sc), "createDataFrame", ., schema) %>%
    sdf_register()
}
