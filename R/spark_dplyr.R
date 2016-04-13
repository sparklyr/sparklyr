#' Connect to Spark.
#'
#' @export
src_spark <- function(master = "local") {
  list(
    master= master
  )
}

#' @export
src_desc.src_spark <- function(con) {
  info <- dbGetInfo(con)

  "spark connection"
}
