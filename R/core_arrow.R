# Changing this file requires running configure.R to rebuild sources and jars.

arrow_write_record_batch <- function(df) {
  record_batch <- get("record_batch", envir = as.environment(asNamespace("arrow")))

  if (packageVersion("arrow") < "0.12") {
    write_record_batch <- get("write_record_batch", envir = as.environment(asNamespace("arrow")))

    record <- record_batch(df)
    write_record_batch(record, raw())
  }
  else if (packageVersion("arrow") <= "0.13") {
    record <- record_batch(df)

    write_arrow <- get("write_arrow", envir = as.environment(asNamespace("arrow")))
    write_arrow(record, raw())
  }
  else {
    environment <- list()
    if (packageVersion("arrow") > "0.14") {
      environment <- list(ARROW_PRE_0_15_IPC_FORMAT = 1)
    }

    withr::with_envvar(environment, {
      record <- record_batch(!!!df)

      write_arrow <- get("write_arrow", envir = as.environment(asNamespace("arrow")))
      write_arrow(record, raw())
    })
  }
}

arrow_record_stream_reader <- function(stream) {
  environment <- list()

  if (packageVersion("arrow") < "0.12") {
    record_batch_stream_reader <- get("record_batch_stream_reader", envir = as.environment(asNamespace("arrow")))
  }
  else {
    record_batch_stream_reader <- get("RecordBatchStreamReader", envir = as.environment(asNamespace("arrow")))
  }

  if (packageVersion("arrow") > "0.14") {
    record_batch_stream_reader <- record_batch_stream_reader$create
    environment <- list(ARROW_PRE_0_15_IPC_FORMAT = 1)
  }

  withr::with_envvar(environment, {
    record_batch_stream_reader(stream)
  })
}

arrow_read_record_batch <- function(reader) {
  environment <- list()

  if (packageVersion("arrow") < "0.12") {
    read_record_batch <- get("read_record_batch", envir = as.environment(asNamespace("arrow")))
  }
  else {
    read_record_batch <- function(reader) reader$read_next_batch()
  }

  if (packageVersion("arrow") > "0.14") {
    environment <- list(ARROW_PRE_0_15_IPC_FORMAT = 1)
  }

  withr::with_envvar(environment, {
    read_record_batch(reader)
  })
}

arrow_as_tibble <- function(record) {
  environment <- list()

  if (packageVersion("arrow") <= "0.13")
    as_tibble <- get("as_tibble", envir = as.environment(asNamespace("arrow")))
  else {
    as_tibble <- get("as.data.frame", envir = as.environment(asNamespace("arrow")))
  }

  if (packageVersion("arrow") > "0.14") {
    environment <- list(ARROW_PRE_0_15_IPC_FORMAT = 1)
  }

  withr::with_envvar(environment, {
    as_tibble(record)
  })
}
