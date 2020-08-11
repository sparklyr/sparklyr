#' @include tidyr_utils.R

check_key <- function(.key) {
  if (!rlang::is_missing(.key)) {
    rlang::warn("`.key` is deprecated")
    .key
  } else {
    "data"
  }
}

#' @importFrom tidyr nest
#' @export
nest.tbl_spark <- function(.data, ..., .names_sep = NULL, .key = lifecycle::deprecated()) {
  if (.data %>% spark_connection() %>% spark_version() < "2.0.0") {
    rlang::abort("`nest.tbl_spark` is only supported in Spark 2.0.0 or higher")
  }

  .key <- check_key(.key)
  group_vars <- dplyr::group_vars(.data)
  if (missing(...)) {
    if (length(group_vars) > 0) {
      nested_cols <- list(data = setdiff(colnames(.data), group_vars))
    } else {
      rlang::warn(paste0(
        "`...` must not be empty for ungrouped data frames.\n",
        "Did you want `", .key, " = everything()`?"
      ))
      nested_cols <- rlang::list2(!!.key := colnames(.data))
    }
  } else {
    cols <- columns(.data)
    nested_cols <- rlang::enquos(...) %>%
      purrr::map(~ names(tidyselect::eval_select(.x, cols)))
  }
  nested_cols <- purrr::map(nested_cols, rlang::set_names)

  if (!is.null(.names_sep)) {
    nested_cols <- purrr::imap(nested_cols, strip_names, .names_sep)
  }
  non_nested_cols <- setdiff(colnames(.data), unlist(nested_cols))

  nesting_sql <- lapply(
    nested_cols,
    function(fields) {
      dplyr::sql(sprintf(
        "COLLECT_LIST(NAMED_STRUCT(%s))",
        lapply(seq_along(fields), function(idx) {
          dst_name <- names(fields)[[idx]]
          src_name <- fields[[idx]]
          sprintf("%s, %s",
            dbplyr::translate_sql_(list(dst_name), con = dbplyr::simulate_dbi()),
            quote_column_name(src_name)
          )
        }) %>%
          paste0(collapse = ", ")
      ))
    }
  )
  names(nesting_sql) <- names(nested_cols)

  out <- dplyr::ungroup(.data)
  out <- do.call(dplyr::group_by, append(list(out), lapply(non_nested_cols, as.symbol)))
  out <- do.call(dplyr::summarize, append(list(out), nesting_sql))
  output_cols <- c(non_nested_cols, names(nested_cols))
  group_vars <- intersect(group_vars, output_cols)
  out <- do.call(dplyr::group_by, append(list(out), lapply(group_vars, as.symbol)))
  handle_empty_lists_sql <- lapply(
    names(nested_cols),
    function(nested_col) {
      nested_col <- quote_column_name(nested_col)
      dplyr::sql(sprintf("IF(SIZE(%s) == 0, NULL, %s)", nested_col, nested_col))
    }
  )
  names(handle_empty_lists_sql) <- names(nested_cols)
  out <- do.call(dplyr::mutate, append(list(out), handle_empty_lists_sql))

  out <- do.call(dplyr::select, append(list(out), lapply(output_cols, as.symbol)))

  out
}
