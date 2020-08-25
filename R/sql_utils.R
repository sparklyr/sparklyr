#' Translate input character vector or symbol to a SQL identifier
#'
#' @description
#' Calls dbplyr::translate_sql_ on the input character vector or symbol to obtain
#' the corresponding SQL identifier that is escaped and quoted properly
#'
#' @keywords internal
#' @export
quote_sql_name <- function(x, con = NULL) {
  UseMethod("quote_sql_name")
}

#' @export
quote_sql_name.name <- function(x, con = NULL) {
  as.character(
    dbplyr::translate_sql_(
      list(x),
      con = con %||% dbplyr::simulate_dbi()
    )
  )
}

#' @export
quote_sql_name.character <- function(x, con = NULL) {
  quote_sql_name(as.symbol(x), con)
}
