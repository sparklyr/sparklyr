dbplyr_uses_ops <- function() {
  older_dbplyr <- packageVersion("dbplyr") <= package_version("2.1.1")
  if(older_dbplyr) stop("No support")
  older_dbplyr
}
