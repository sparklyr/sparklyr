arrow_enabled <- function() {
  "package:arrow" %in% search()
}

arrow_batch <- function(df)
{
  if (!arrow_enabled()) {
    stop("The 'arrow' package is not available, use 'library(arrow)' to enable the arrow serializer.")
  }

  record_batch <- get("record_batch", envir = as.environment("package:arrow"))

  file <- tempfile(fileext = ".arrow")
  record <- record_batch(df)
  record$to_file(file)

  readBin(con = file, "raw", n = 10^6)
}

arrow_copy_to <- function(sc, df, parallelism = 8L, serializer = "arrow")
{
  # serialize to arrow
  bytes <- arrow_batch(df)

  # create batches data frame
  batches <- list(bytes)

  # build schema
  schema <- sparklyr:::spark_data_build_types(sc, lapply(df, class))

  # load arrow file in scala
  rdd <- invoke_static(sc, "sparklyr.ArrowHelper", "javaRddFromBinaryBatches", spark_context(sc), batches, parallelism)
  sdf <- invoke_static(sc, "sparklyr.ArrowConverters", "toDataFrame", rdd, schema, spark_session(sc))

  sdf_register(sdf)
}
