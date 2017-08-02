#' @export
gather_.tbl_spark <- function(data,
                              key_col,
                              value_col,
                              gather_cols,
                              na.rm = FALSE,
                              convert = FALSE,
                              factor_key = FALSE)
{
  # determine id columns
  columns <- tbl_vars(data)
  id_cols <- setdiff(columns, gather_cols)

  # extract the columns we need, alongside the key column
  parts <- lapply(gather_cols, function(gather_col) {
    data %>%
      select_(.dots = c(id_cols, value = gather_col)) %>%
      mutate(variable = gather_col)
  })

  # stack the value columns
  stacked <- Reduce(function(lhs, rhs) {
    joined <- full_join(lhs, rhs, c(id_cols, "variable"))
    lhs <- joined %>%
      mutate(value = ifelse(is.na(value.x), value.y, value.x)) %>%
      select_(.dots = c(id_cols, "variable", "value"))
  }, parts)

  # set names
  nm <- stats::setNames(
    c(id_cols, "variable", "value"),
    c(id_cols, key_col, value_col)
  )

  stacked %>% select_(.dots = as.list(nm))
}
