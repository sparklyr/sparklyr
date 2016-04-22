#' Connect to Spark for Dplyr.
#'
#' @import dplyr
#' @export
src_spark <- function(master = "local",
                      appName = "dplyrspark") {
  setup_local()
  con <- dbConnect(DBISpark())
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
sql_drop_table.src_spark <- function(con, name) {
  dbRemoveTable(con, name)
}

#' @export
copy_to.src_spark <- function(con, df, name) {
  dbWriteTable(con$con, name, df)
}

#' @export
sql_create_index.src_spark <- function(...) {
}

#' @export
sql_analyze.src_spark <- function(...) {
}

