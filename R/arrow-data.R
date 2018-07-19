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
  invoke_static(sc, "ArrowUtils", spark_context(sc), batches, parallelism)
}
