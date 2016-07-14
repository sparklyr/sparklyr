#' @export
rbind.tbl_spark <- function(..., deparse.level = 1, name = random_string("sparklyr_tmp_"))
{
  dots <- list(...)
  n <- length(dots)
  self <- dots[[1]]

  if (n == 1)
    return(self)

  sdf <- spark_dataframe(self)
  for (i in 2:n)
    sdf <- invoke(sdf, "unionAll", spark_dataframe(dots[[i]]))

  sdf_register(sdf, name = name)
}
