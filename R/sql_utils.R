# helper method returning a minimal R dataframe containing the same set of
# column names as `sdf` does
columns <- function(sdf) {
  columns <- lapply(
    colnames(sdf),
    function(column) {
      v <- list(NA)
      names(v) <- column
      v
    }
  )
  do.call(data.frame, columns)
}

# helper method to ensure a SQL column name is escaped and quoted properly
quote_column_name <- function(column_name, con = NULL) {
  as.character(
    dbplyr::translate_sql_(
      list(as.symbol(column_name)),
      con = con %||% dbplyr::simulate_dbi()
    )
  )
}
