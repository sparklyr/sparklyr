arrow_write_record_batch <- function(df) {
  record_batch <- get("record_batch", envir = as.environment(asNamespace("arrow")))
  record <- record_batch(df)

  record_writer <- get("BufferOutputStream", envir = as.environment(asNamespace("arrow")))()
  write_record_batch_inst <- get("RecordBatchStreamWriter", envir = as.environment(asNamespace("arrow")))(record_writer, record$schema)

  write_record_batch_inst$write_batch(record)
  write_record_batch_inst$close()

  data <- record_writer$getvalue()

  bytes <- raw(data$size)
  buffer <- get("buffer", envir = as.environment(asNamespace("arrow")))
  fixed_writer <- get("FixedSizeBufferWriter", envir = as.environment(asNamespace("arrow")))(buffer(bytes))
  write_record_batch_inst <- get("RecordBatchStreamWriter", envir = as.environment(asNamespace("arrow")))(fixed_writer, record$schema)

  write_record_batch_inst$write_batch(record)
  write_record_batch_inst$close()

  bytes
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
