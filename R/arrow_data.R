arrow_enabled <- function(sc, object) {
  enabled <- spark_config_value(sc, "sparklyr.arrow", "package:arrow" %in% search())
  if (!enabled) {
    enabled
  }
  else {
    arrow_enabled_object(object)
  }
}

arrow_enabled_object <- function(object) {
  UseMethod("arrow_enabled_object")
}

arrow_enabled_object.default <- function(object) {
  TRUE
}

arrow_enabled_object.tbl_spark <- function(object) {
  sdf <- spark_dataframe(object)
  arrow_enabled_object(sdf)
}

arrow_enabled_object.spark_jobj <- function(object) {
  unsupported_expr <- ".Vector|ArrayType|StructType"
  unsupported <- object %>%
    sdf_schema() %>%
    Filter(function(x) grepl(unsupported_expr, x$type), .)
  enabled <- length(unsupported) == 0
  if (!enabled) warning("Arrow disabled due to columns: ", paste(names(unsupported), collapse = ", "))

  enabled
}

arrow_enabled_dataframe_schema <- function(types) {
  unsupported_expr <- "^$"
  unsupported <- Filter(function(e) grepl(unsupported_expr , e), types)

  enabled <- length(unsupported) == 0
  if (!enabled) warning("Arrow disabled due to columns: ", paste(names(unsupported), collapse = ", "))

  enabled
}

arrow_enabled_object.data.frame <- function(object) {
  arrow_enabled_dataframe_schema(sapply(object, function(e) class(e)[[1]]))
}

arrow_batch <- function(df)
{
  record_batch <- get("record_batch", envir = as.environment(asNamespace("arrow")))
  write_record_batch <- get("write_record_batch", envir = as.environment(asNamespace("arrow")))

  record <- record_batch(df)
  write_record_batch(record, raw())
}

arrow_read_stream <- function(stream)
{
  record_batch_stream_reader <- get("record_batch_stream_reader", envir = as.environment(asNamespace("arrow")))
  read_record_batch <- get("read_record_batch", envir = as.environment(asNamespace("arrow")))

  reader <- record_batch_stream_reader(stream)
  record_entry <- read_record_batch(reader)

  entries <- list()
  while (!is.null(record_entry)) {
    entries[[length(entries) + 1]] <- tibble::as_tibble(record_entry)
    record_entry <- read_record_batch(reader)
  }

  entries
}

arrow_copy_to <- function(sc, df, parallelism)
{
  # build schema
  first_df <- if (identical(class(df), "list")) df[[1]] else df
  schema <- spark_data_build_types(sc, lapply(first_df, class))

  # mark as arrow stream
  class(df) <- c("arrow_stream", class(df))

  # load arrow file in scala
  rdd <- invoke_static(sc, "sparklyr.ArrowHelper", "javaRddFromBinaryBatches", spark_context(sc), df, parallelism)
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
