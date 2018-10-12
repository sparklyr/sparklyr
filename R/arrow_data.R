arrow_enabled <- function(sc) {
  spark_config_value(sc, "sparklyr.arrow", "package:arrow" %in% search())
}

arrow_batch <- function(df)
{
  record_batch <- get("record_batch", envir = as.environment(asNamespace("arrow")))

  record <- record_batch(df)
  record$to_stream()
}

arrow_read_stream <- function(stream)
{
  read_record_batch_stream <- get("read_record_batch_stream", envir = as.environment(asNamespace("arrow")))

  read_record_batch_stream(stream)
}

arrow_copy_to <- function(sc, df, parallelism = 8L, serializer = "arrow")
{
  # serialize to arrow
  bytes <- arrow_batch(df)

  # create batches data frame
  batches <- list(bytes)

  # build schema
  schema <- sparklyr:::spark_data_build_types(sc, lapply(df, class))

  # load arrow file in scala
  rdd <- invoke_static(sc, "sparklyr.ArrowHelper", "javaRddFromBinaryBatches", spark_context(sc), batches, parallelism)
  sdf <- invoke_static(sc, "sparklyr.ArrowConverters", "toDataFrame", rdd, schema, spark_session(sc))

  sdf
}

arrow_collect <- function(tbl, ...)
{
  sc <- spark_connection(tbl)
  sdf <- spark_dataframe(tbl)
  session <- spark_session(sc)

  time_zone <- spark_session(sc) %>% invoke("sessionState") %>% invoke("conf") %>% invoke("sessionLocalTimeZone")

  invoke_static(sc, "sparklyr.ArrowConverters", "toArrowBatchRdd", sdf, session, time_zone) %>%
    arrow_read_stream() %>%
    dplyr::bind_rows()
}
