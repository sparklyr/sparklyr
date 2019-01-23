arrow_write_record_batch <- function(df) {
  if (packageVersion("arrow") < "0.12") {
    record_batch <- get("record_batch", envir = as.environment(asNamespace("arrow")))
    write_record_batch <- get("write_record_batch", envir = as.environment(asNamespace("arrow")))

    record <- record_batch(df)
    write_record_batch(record, raw())
  }
  else {
    record_batch <- get("record_batch", envir = as.environment(asNamespace("arrow")))
    record <- record_batch(df)

    write_arrow <- get("write_arrow", envir = as.environment(asNamespace("arrow")))
    write_arrow(record, raw())

  }
}

arrow_record_stream_reader <- function(stream) {
  if (packageVersion("arrow") < "0.12") {
    record_batch_stream_reader <- get("record_batch_stream_reader", envir = as.environment(asNamespace("arrow")))
  }
  else {
    record_batch_stream_reader <- get("RecordBatchStreamReader", envir = as.environment(asNamespace("arrow")))
  }

  record_batch_stream_reader(stream)
}

arrow_read_record_batch <- function(reader) {
  if (packageVersion("arrow") < "0.12") {
    read_record_batch <- get("read_record_batch", envir = as.environment(asNamespace("arrow")))
  }
  else {
    read_record_batch <- function(reader) reader$read_next_batch()
  }

  read_record_batch(reader)
}

arrow_as_tibble <- function(record) {
  as_tibble <- get("as_tibble", envir = as.environment(asNamespace("arrow")))

  as_tibble(record)
}
