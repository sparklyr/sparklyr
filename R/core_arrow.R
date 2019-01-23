arrow_write_record_batch <- function(df) {
  record_batch <- get("record_batch", envir = as.environment(asNamespace("arrow")))
  record <- record_batch(df)

  write_arrow <- get("write_arrow", envir = as.environment(asNamespace("arrow")))
  write_arrow(record, raw())
}

arrow_read_record_batch <- function(batch) {
  record_batch_stream_reader <- get("RecordBatchStreamReader", envir = as.environment(asNamespace("arrow")))()
  read_record_batch <- function(reader) reader$read_next_batch()

  reader <- record_batch_stream_reader(batch)
  read_record_batch(reader)
}

arrow_as_tibble <- function(record) {
  as_tibble <- get("as_tibble", envir = as.environment(asNamespace("arrow")))

  as_tibble(record)
}
