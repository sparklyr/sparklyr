# Changing this file requires running configure.R to rebuild sources and jars.

# Someday this setting may depend on the Spark version
arrow_env_vars <- list(ARROW_PRE_0_15_IPC_FORMAT = 1)

get_arrow_function <- function(x) get(x, as.environment(asNamespace("arrow")))

arrow_write_record_batch <- function(df) {
  if ("write_to_raw" %in% ls(envir = asNamespace("arrow"))) {
    # New in arrow 0.17: takes a data.frame and returns a raw buffer with Arrow data
    write_to_raw <- get_arrow_function("write_to_raw")
    # Also new in 0.17: arrow doesn't hardcode a GMT timezone anymore
    # so set the local timezone to any POSIXt columns that don't have one set
    # https://github.com/sparklyr/sparklyr/issues/2439
    df[] <- lapply(df, function(x) {
      if (inherits(x, "POSIXt") && is.null(attr(x, "tzone"))) {
        attr(x, "tzone") <- Sys.timezone()
      }
      x
    })

    return(write_to_raw(df, format = "stream"))
  }

  record_batch <- get_arrow_function("record_batch")
  if (packageVersion("arrow") < "0.12") {
    write_record_batch <- get_arrow_function("write_record_batch")

    record <- record_batch(df)
    write_record_batch(record, raw())
  }
  else if (packageVersion("arrow") <= "0.13") {
    record <- record_batch(df)

    write_arrow <- get_arrow_function("write_arrow")
    write_arrow(record, raw())
  }
  else {
    withr::with_envvar(arrow_env_vars, {
      record <- record_batch(!!!df)

      write_arrow <- get_arrow_function("write_arrow")
      write_arrow(record, raw())
    })
  }
}

arrow_record_stream_reader <- function(stream) {
  if (packageVersion("arrow") < "0.12") {
    record_batch_stream_reader <- get_arrow_function("record_batch_stream_reader")
  }
  else {
    record_batch_stream_reader <- get_arrow_function("RecordBatchStreamReader")
  }

  if (packageVersion("arrow") > "0.14") {
    record_batch_stream_reader <- record_batch_stream_reader$create
  }

  withr::with_envvar(arrow_env_vars, {
    record_batch_stream_reader(stream)
  })
}

arrow_read_record_batch <- function(reader) {
  if (packageVersion("arrow") < "0.12") {
    read_record_batch <- get_arrow_function("read_record_batch")
  }
  else {
    read_record_batch <- function(reader) reader$read_next_batch()
  }

  withr::with_envvar(arrow_env_vars, {
    read_record_batch(reader)
  })
}

arrow_as_tibble <- function(record) {
if (packageVersion("arrow") <= "0.13")
    as_tibble <- get("as_tibble", envir = as.environment(asNamespace("arrow")))
  else {
    as_tibble <- get("as.data.frame", envir = as.environment(asNamespace("arrow")))
  }

  withr::with_envvar(arrow_env_vars, {
    as_tibble(record)
  })
}
