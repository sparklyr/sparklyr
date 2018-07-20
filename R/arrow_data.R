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

as_arrow_python <- function(df)
{
  pa <- reticulate::import("pyarrow")

  buf <- pa$serialize(df)$to_buffer()
  reader <- pa$BufferReader(buf)

  builtins <- reticulate::import_builtins()
  builtins$bytearray(buf)
}

arrow_copy_to <- function(sc, df, parallelism = 8L, serializer = "python")
{
  serializers <- list(
    feather = as_arrow_feather,
    python = as_arrow_python
  )

  # serialize to arrow
  bytes <- serializers[[serializer]](df)

  # create batches data frame
  batches <- list(bytes)

  # load arrow file in scala
  rdd <- invoke_static(sc, "sparklyr.ArrowHelper", "javaRddFromBinaryBatches", spark_context(sc), batches, parallelism)
  schema <- sparklyr:::spark_data_build_types(sc, lapply(df, class))
  sdf <- invoke_static(sc, "sparklyr.ArrowConverters", "toDataFrame", rdd, schema, spark_session(sc))

  sdf_register(sdf)
}
