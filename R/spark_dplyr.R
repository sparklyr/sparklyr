#' Connect to Spark for Dplyr.
#'
#' @import dplyr
#' @export
src_spark <- function(master = "local",
                      appName = "splyr") {
  con <- start_shell()

  con$sc <- spark_api_create_context(con, master, appName)
  if (identical(con$sc, NULL)) {
    stop("Failed to create Spark context")
  }

  con$sql <- spark_api_create_sql_context(con)
  if (identical(con$sc, NULL)) {
    stop("Failed to create SQL context")
  }

  attr(con, "class") <- c("SparkConnection")

  src_sql("spark", con)
}

#' @export
src_translate_env.src_spark <- function(x) {
  nyi <- function(...) stop("Currently not supported")

  dplyr::sql_variant(
    dplyr::sql_translator(
      .parent = dplyr::base_scalar,

      # Casting
      as.logical = dplyr::sql_prefix("string"),
      as.numeric = dplyr::sql_prefix("string"),
      as.double = dplyr::sql_prefix("string"),
      as.integer = dplyr::sql_prefix("string"),
      as.character = dplyr::sql_prefix("string"),

      # Date/time
      Sys.date = nyi,
      Sys.time = nyi,

      # Regular expressions
      grepl = nyi,
      gsub = nyi,

      # stringr equivalents
      str_detect = nyi,
      str_extract = nyi,
      str_replace = nyi
    ),
    dplyr::sql_translator(
      .parent = dplyr::base_agg,
      n = nyi,
      "%||%" = nyi,
      sd =nyi
    ),
    dplyr::sql_translator(
      .parent = dplyr::base_win,
      mean  = nyi,
      sum   = nyi,
      min   = nyi,
      max   = nyi,
      n     = nyi,
      cummean = nyi,
      cumsum  = nyi,
      cummin  = nyi,
      cummax  = nyi
    )
  )
}

#' @export
src_desc.src_spark <- function(con) {
  "spark connection"
}

#' @export
db_explain.src_spark <- function(con) {
  ""
}

#' @export
tbl.src_spark <- function(src, from, ...) {
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
db_data_type.src_spark <- function(...) {
}

#' @export
sql_begin.src_spark <- function(...) {
}

#' @export
sql_commit.src_spark <- function(...) {
}

#' @export
sql_rollback.src_spark <- function(...) {
}

#' @export
sql_create_table.src_spark <- function(...) {
}

#' @export
sql_insert_into.src_spark <- function(...) {
}

#' @export
sql_drop_table.src_spark <- function(name) {
  spark_api_sql(con, paste("DROP TABLE '", name, "'", sep = ""))
}

#' @export
sql_create_index.src_spark <- function(...) {
}

#' @export
sql_analyze.src_spark <- function(...) {
}

