#' @rawNamespace S3method(rbind,tbl_spark)
rbind.tbl_spark <- function(..., deparse.level = 1, name = random_string("sparklyr_tmp_"))
{
  dots <- list(...)
  n <- length(dots)
  self <- dots[[1]]

  if (n == 1)
    return(self)

  sdf <- spark_dataframe(self)

  # NOTE: 'unionAll' was deprecated in Spark 2.0.0, but 'unionAll'
  # is not available for DataFrames in older versions of Spark, so
  # provide a bit of indirection based on Spark version
  sc <- spark_connection(sdf)
  version <- spark_version(sc)
  method <- if (version < "2.0.0") "unionAll" else "union"

  for (i in 2:n)
    sdf <- invoke(sdf, method, spark_dataframe(dots[[i]]))

  sdf_register(sdf, name = name)
}

mutate_names <- function(x, value) {
  sdf <- spark_dataframe(x)
  renamed <- invoke(sdf, "toDF", as.list(value))
  sdf_register(renamed, name = as.character(x$ops$x))
}

#' @export
`names<-.tbl_spark` <- function(x, value) {
  mutate_names(x, value)
}
