# Helper functions to support dplyr sql operations

#' @export
#' @importFrom dbplyr sql_build
#' @importFrom dbplyr select_query
sql_build.op_sample_n <- function(op, con, ...) {
  select_query(
    from = sql(
      sql_render(op$x, con = con),
      sql(paste0(" TABLESAMPLE (",
                 as.integer(op$args$size),
                 " rows) ", collapse = ""))
    ) %>%
      as.character() %>%
      paste0(collapse = "") %>%
      sql(),
    select = build_sql("*")
  )
}

#' @export
#' @importFrom dbplyr sql_build
#' @importFrom dbplyr select_query
sql_build.op_sample_frac <- function(op, con, ...) {
  select_query(
    from = sql(
      sql_render(op$x, con = con),
      sql(paste0(" TABLESAMPLE (",
                 op$args$size * 100,
                 " PERCENT)", collapse = ""))
    ) %>%
      as.character() %>%
      paste0(collapse = "") %>%
      sql(),
    select = build_sql("*")
  )
}
