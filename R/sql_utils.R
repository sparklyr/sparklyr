# helper method to ensure a SQL column name is escaped and quoted properly
quote_column_name <- function(column_name, con = NULL) {
  UseMethod("quote_column_name")
}

quote_column_name.name <- function(column_name, con = NULL) {
  as.character(
    dbplyr::translate_sql_(
      list(column_name),
      con = con %||% dbplyr::simulate_dbi()
    )
  )
}

quote_column_name.character <- function(column_name, con = NULL) {
  quote_column_name(as.symbol(column_name), con)
}
