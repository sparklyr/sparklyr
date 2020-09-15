#' @include tidyr_utils.R
NULL

#' @importFrom tidyr unite
#' @export
unite.tbl_spark <- function(data, col, ..., sep = "_", remove = TRUE, na.rm = FALSE) {
  col <- rlang::as_string(rlang::ensym(col))

  if (rlang::dots_n(...) == 0) {
    src_cols <- colnames(data)
  } else {
    src_cols <- names(
      tidyselect::eval_select(rlang::expr(c(...)), replicate_colnames(data))
    )
  }

  output_cols <- colnames(data)
  if (remove) {
    output_cols <- setdiff(output_cols, src_cols)
  }

  first_pos <- which(colnames(data) %in% src_cols)[[1]]
  output_cols <- append(output_cols, col, after = first_pos - 1L)

  concat_ws_args <- lapply(
    src_cols,
    function(col) {
      col <- quote_sql_name(col)
      if (!na.rm) {
        paste0("IF(ISNULL(", col, "), \"NA\", ", col, ")")
      } else {
        col
      }
    }
  )
  sql <- paste0(
    append(list(dbplyr::translate_sql(!!sep)), concat_ws_args),
    collapse = ", "
  ) %>%
    paste0("CONCAT_WS(", ., ")") %>%
    dplyr::sql() %>%
    list()
  names(sql) <- col

  data %>>%
    dplyr::mutate %@% sql %>%
    dplyr::ungroup(setdiff(colnames(data), output_cols)) %>%
    update_group_vars(data, ., output_cols) %>>%
    dplyr::select %@% lapply(output_cols, as.symbol)
}
