arrow_python_install <- function()
{
  reticulate::py_install("pyarrow")
}

as_arrow_feather <- function(df)
{
  featherFile <- tempfile(fileext = ".feather")
  feather::write_feather(df, featherFile)
  con = file(featherFile, "rb")
  readBin(con, "raw", n = 1e10)
}

as_arrow_buffers <- function(df)
{
  pa <- reticulate::import("pyarrow")

  buf <- pa$serialize(df)$to_buffer()
  reader <- pa$BufferReader(buf)

  builtins <- reticulate::import_builtins()
  builtins$bytearray(buf)
}

arrow_schema <- function(df)
{
  pdf <- pa$Table$from_pandas(df)
  pdf$schema
}

arrow_type <- function(object, colname)
{
  type <- switch(
    typeof(object),
    logical =   pa$bool_(),
    integer =   pa$int32(),
    double =    pa$float64(),
    character = pa$string()
  )

  pa$field(colname, type = type)
}

as_arrow_python <- function(df)
{
  io <- reticulate::import("io")
  pa <- reticulate::import("pyarrow")

  pdCols <- lapply(df, function(col) pa$Array$from_pandas(col))
  batch <- pa$RecordBatch$from_arrays(
    lapply(1:length(pdCols), function(i) pdCols[[i]]),
    as.list(names(df))
  )

  sink <- io$BytesIO()
  schema <- pa$schema(
    lapply(colnames(df), function(colname) arrow_type(df[[colname]], colname))
  )
  writer <- pa$RecordBatchFileWriter(sink, schema)
  writer$write_batch(batch)
  writer$close()
  builtins$bytearray(sink$getvalue())
}

arrow_copy_to <- function(sc, df, parallelism = 8L, serializer = "python")
{
  serializers <- list(
    feather = as_arrow_feather,
    buffers = as_arrow_buffers,
    python = as_arrow_python
  )

  # serialize to arrow
  bytes <- serializers[[serializer]](df)

  # create batches data frame
  batches <- list(bytes)

  # build schema
  schema <- sparklyr:::spark_data_build_types(sc, lapply(df, class))

  # load arrow file in scala
  rdd <- invoke_static(sc, "sparklyr.ArrowHelper", "javaRddFromBinaryBatches", spark_context(sc), batches, parallelism)
  sdf <- invoke_static(sc, "sparklyr.ArrowConverters", "toDataFrame", rdd, schema, spark_session(sc))

  sdf_register(sdf)
}
