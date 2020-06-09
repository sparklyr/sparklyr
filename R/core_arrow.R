# Changing this file requires running update_embedded_sources.R to rebuild sources and jars.

arrow_write_record_batch <- function(df, spark_version_number = NULL) {
  arrow_env_vars <- list()
  if (!is.null(spark_version_number) && spark_version_number < "3.0") {
    # Spark < 3 uses an old version of Arrow, so send data in the legacy format
    arrow_env_vars$ARROW_PRE_0_15_IPC_FORMAT <- 1
  }

  withr::with_envvar(arrow_env_vars, {
    # New in arrow 0.17: takes a data.frame and returns a raw buffer with Arrow data
    if ("write_to_raw" %in% ls(envir = asNamespace("arrow"))) {
      # Fixed in 0.17: arrow doesn't hardcode a GMT timezone anymore
      # so set the local timezone to any POSIXt columns that don't have one set
      # https://github.com/sparklyr/sparklyr/issues/2439
      df[] <- lapply(df, function(x) {
        if (inherits(x, "POSIXt") && is.null(attr(x, "tzone"))) {
          attr(x, "tzone") <- Sys.timezone()
        }
        x
      })
      arrow::write_to_raw(df, format = "stream")
    } else {
      arrow::write_arrow(arrow::record_batch(!!!df), raw())
    }
  })
}

arrow_record_stream_reader <- function(stream) {
  arrow::RecordBatchStreamReader$create(stream)
}

arrow_read_record_batch <- function(reader) reader$read_next_batch()

arrow_as_tibble <- function(record) as.data.frame(record)
