# Changing this file requires running update_embedded_sources.R to rebuild sources and jars.

arrow_write_record_batch <- function(df, spark_version_number = NULL) {
  arrow_env_vars <- list()
  if (!is.null(spark_version_number) && spark_version_number < "3.0") {
    arrow_env_vars <- list(ARROW_PRE_0_15_IPC_FORMAT = 1)
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
    } else if (packageVersion("arrow") < "0.12") {
      arrow::write_record_batch(arrow::record_batch(df), raw())
    } else if (packageVersion("arrow") <= "0.13") {
      arrow::write_arrow(arrow::record_batch(df), raw())
    } else {
      arrow::write_arrow(arrow::record_batch(!!!df), raw())
    }
  })
}

arrow_record_stream_reader <- function(stream) {
  if (packageVersion("arrow") < "0.12") {
    record_batch_stream_reader <- arrow::record_batch_stream_reader
  } else {
    record_batch_stream_reader <- arrow::RecordBatchStreamReader
  }

  if (packageVersion("arrow") > "0.14") {
    record_batch_stream_reader <- record_batch_stream_reader$create
  }
  record_batch_stream_reader(stream)
}

arrow_read_record_batch <- function(reader) {
  if (packageVersion("arrow") < "0.12") {
    arrow::read_record_batch(reader)
  } else {
    reader$read_next_batch()
  }
}

arrow_as_tibble <- function(record) {
  if (packageVersion("arrow") <= "0.13") {
    arrow::as_tibble(record)
  } else {
    arrow::as.data.frame(record)
  }
}
