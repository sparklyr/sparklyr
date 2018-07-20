arrow_copy_to <- function(sc, df, parallelism = 8L)
{
  # save as a feather file and reload, need to consider PR to retrieve in-memory
  featherFile <- tempfile(fileext = ".feather")
  feather::write_feather(df, featherFile)
  con = file(featherFile, "rb")
  bytes <- readBin(con, "raw", n = 1e10)

  # create batches data frame
  batches <- list(bytes)

  # load arrow file in scala
  rdd <- invoke_static(sc, "sparklyr.ArrowHelper", "javaRddFromBinaryBatches", spark_context(sc), batches, parallelism)
  schema <- spark_data_build_types(sc, lapply(df, class))
  sdf <- invoke_static(sc, "sparklyr.ArrowRowIterator", "toDataFrame", rdd, schema, spark_session(sc))
}
