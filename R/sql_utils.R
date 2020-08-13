# helper method to ensure a SQL column or table name is escaped and quoted
# properly
quote_sql_name <- function(x, con = NULL) {
  UseMethod("quote_sql_name")
}

quote_sql_name.name <- function(x, con = NULL) {
  as.character(
    dbplyr::translate_sql_(
      list(x),
      con = con %||% dbplyr::simulate_dbi()
    )
  )
}

quote_sql_name.character <- function(x, con = NULL) {
  quote_sql_name(as.symbol(x), con)
}
