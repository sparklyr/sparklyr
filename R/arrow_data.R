arrow_enabled <- function(sc) {
  spark_config_value(sc, "sparklyr.arrow", "package:arrow" %in% search())
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
  while (!record_entry$is_null()) {
    entries[[length(entries) + 1]] <- tibble::as_tibble(record_entry)
    record_entry <- read_record_batch(reader)
  }

  entries
}

arrow_copy_to <- function(sc, df, parallelism = 8L, serializer = "arrow")
{
  # replace factors with characters
  if (any(sapply(df, is.factor))) {
    df <- dplyr::as_data_frame(lapply(df, function(x) if(is.factor(x)) as.character(x) else x))
  }

  # serialize to arrow
  bytes <- arrow_batch(df)

  # create batches data frame
  batches <- list(bytes)

  # build schema
  schema <- spark_data_build_types(sc, lapply(df, class))

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
