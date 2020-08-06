#' @include sql_utils.R
#' @include tidyr_utils.R
#' @include utils.R

validate_args <- function(into, sep) {
  if (!is.character(into))
    rlang::abort("`into` must be a character vector")

  if (!is.numeric(sep) && !is.character(sep))
    rlang::abort("`sep` must be either numeric or character")

  if (is.numeric(sep) && !identical(length(sep) + 1L, length(into)))
    rlang::abort("The length of `sep` should be one less than `into`.")
}

strsep_to_sql <- function(col, into, sep) {
  splitting_idx <- lapply(
    sep,
    function(idx) {
      if (idx >= 0)
        as.character(idx)
      else
        sprintf("ARRAY_MAX(ARRAY(0, CHAR_LENGTH(%s) + %d))", col, idx)
    }
  )
  pos <-
    list("0") %>%
      append(splitting_idx) %>%
      append(sprintf("CHAR_LENGTH(%s)", col))
  sql <- lapply(
    seq(length(pos) - 1),
    function(i) {
      from <- sprintf("%s + 1", pos[[i]])
      len <- sprintf("%s - %s", pos[[i + 1]], pos[[i]])

      dplyr::sql(sprintf("SUBSTR(%s, %s, %s)", col, from, len))
    }
  )
  names(sql) <- into

  sql[!is.na(into)]
}

str_split_fixed_to_sql <- function(tmp_col, col, sep, n, extra, fill) {
  if (identical(extra, "error")) {
    rlang::warn("`extra = \"error\"` is deprecated. Please use `extra = \"warn\"` instead")
    extra <- "warn"
  }

  extra <- rlang::arg_match(extra, c("warn", "merge", "drop"))
  fill <- rlang::arg_match(fill, c("warn", "left", "right"))

  sep <- dbplyr::translate_sql_(list(sep), con = dbplyr::simulate_dbi())
  limit <- if (identical(extra, "merge")) n else -1L
  sql <- list(dplyr::sql(sprintf("SPLIT(%s, %s, %d)", col, sep, limit)))
  names(sql) <- tmp_col

  sql
}

#' @importFrom tidyr separate
#' @export
separate.tbl_spark <- function(data, col, into, sep = "[^0-9A-Za-z]+",
                               remove = TRUE, extra = "warn", fill = "warn", ...) {
  if (data %>% spark_connection() %>% spark_version() < "3.0.0")
    rlang::abort("`separate.tbl_spark` is only supported in Spark 3.0.0 or higher")

  check_present(col)
  validate_args(into, sep)

  var <- tidyselect::vars_pull(colnames(data), !! rlang::enquo(col))

  col <- quote_column_name(var)
  if (is.numeric(sep)) {
    sql <- strsep_to_sql(col, into, sep)
    out <- do.call(dplyr::mutate, append(list(data), sql))
  } else {
    tmp_col <- random_string("__tidyr_separate_tmp_")
    n <- length(into)
    split_str_sql <- str_split_fixed_to_sql(tmp_col, col, sep, n, extra, fill)
    tmp_col <- quote_column_name(tmp_col)
    fill_left <- identical(fill, "left")
    assign_results_sql <- lapply(
      seq_along(into),
      function(idx) {
        dplyr::sql(
          if (fill_left)
            sprintf(
              "IF(%d <= %d - SIZE(%s), NULL, ELEMENT_AT(%s, %d - ARRAY_MAX(ARRAY(0, %d - SIZE(%s)))))",
              idx, n, tmp_col, tmp_col, idx, n, tmp_col
            )
          else
            sprintf(
              "IF(%d <= SIZE(%s), ELEMENT_AT(%s, %d), NULL)", idx, tmp_col, tmp_col, idx
            )
        )
      }
    )
    names(assign_results_sql) <- into
    assign_results_sql <- assign_results_sql[!is.na(into)]
    out <- do.call(dplyr::mutate, append(list(data), split_str_sql))
    out <- do.call(dplyr::mutate, append(list(out), assign_results_sql))
  }

  preserved <- setdiff(dplyr::group_vars(data), into)
  preserved <- setdiff(preserved, var)
  output_cols <- if (remove) into else union(colnames(data), into)
  output_cols <- union(preserved, output_cols)
  out <- update_group_vars(data, out, preserved)
  output_cols <- output_cols %>% lapply(as.symbol)
  do.call(dplyr::select, append(list(out), output_cols))
}
