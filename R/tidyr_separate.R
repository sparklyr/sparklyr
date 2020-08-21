#' @include sql_utils.R
#' @include tidyr_utils.R
#' @include utils.R
NULL

validate_args <- function(into, sep) {
  if (!is.character(into)) {
    rlang::abort("`into` must be a character vector")
  }

  if (!is.numeric(sep) && !is.character(sep)) {
    rlang::abort("`sep` must be either numeric or character")
  }

  if (is.numeric(sep) && !identical(length(sep) + 1L, length(into))) {
    rlang::abort("The length of `sep` should be one less than `into`.")
  }
}

strsep_to_sql <- function(col, into, sep) {
  splitting_idx <- lapply(
    sep,
    function(idx) {
      if (idx >= 0) {
        as.character(idx)
      } else {
        sprintf("ARRAY_MAX(ARRAY(0, CHAR_LENGTH(%s) + %d))", col, idx)
      }
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

str_split_fixed_to_sql <- function(substr_arr_col, col, sep, n, extra, fill, impl) {
  if (identical(extra, "error")) {
    rlang::warn("`extra = \"error\"` is deprecated. Please use `extra = \"warn\"` instead")
    extra <- "warn"
  }

  extra <- rlang::arg_match(extra, c("warn", "merge", "drop"))
  fill <- rlang::arg_match(fill, c("warn", "left", "right"))

  sep <- dbplyr::translate_sql_(list(sep), con = dbplyr::simulate_dbi())
  limit <- if (identical(extra, "merge")) n else -1L
  sql <- list(dplyr::sql(sprintf("%s(%s, %s, %d)", impl, col, sep, limit)))
  names(sql) <- substr_arr_col

  sql
}

process_warnings <- function(out, substr_arr_col, n, extra, fill) {
  if (identical(extra, "warn") || identical(fill, "warn")) {
    output_cols <- colnames(out)
    tmp_tbl_name <- random_string("__tidyr_separate_tmp_tbl_")
    row_num <- random_string("__tidyr_separate_row_num_")
    row_num_sql <- list(dplyr::sql("ROW_NUMBER() OVER (ORDER BY (SELECT 0))"))
    names(row_num_sql) <- row_num
    out <- out %>>%
      dplyr::mutate %@% row_num_sql %>%
      dplyr::compute(name = tmp_tbl_name)
    substr_arr_col_sql <- sprintf(
      "%s.%s",
      quote_sql_name(tmp_tbl_name),
      substr_arr_col
    )
    if (identical(extra, "warn")) {
      pred <- sprintf("SIZE(%s) > %d", substr_arr_col_sql, n)
      rows <- out %>%
        dplyr::filter(dplyr::sql(pred)) %>%
        dplyr::select(rlang::sym(row_num)) %>%
        collect()
      rows <- rows[[row_num]]
      if (length(rows) > 0) {
        sprintf(
          "Expected %d piece(s). Additional piece(s) discarded in %d row(s) [%s].",
          n,
          length(rows),
          paste0(rows, collapse = ", ")
        ) %>%
          rlang::warn()
      }
    }
    if (identical(fill, "warn")) {
      pred <- sprintf("SIZE(%s) < %d", substr_arr_col_sql, n)
      rows <- out %>%
        dplyr::filter(dplyr::sql(pred)) %>%
        dplyr::select(rlang::sym(row_num)) %>%
        collect()
      rows <- rows[[row_num]]
      if (length(rows) > 0) {
        sprintf(
          "Expected %d piece(s). Missing piece(s) filled with NULL value(s) in %d row(s) [%s].",
          n,
          length(rows),
          paste0(rows, collapse = ", ")
        ) %>%
          rlang::warn()
      }
    }
  }

  out
}

#' @importFrom tidyr separate
#' @export
separate.tbl_spark <- function(data, col, into, sep = "[^0-9A-Za-z]+",
                               remove = TRUE, extra = "warn", fill = "warn", ...) {
  check_present(col)
  validate_args(into, sep)

  sc <- spark_connection(data)
  if (spark_version(sc) < "2.4.0") {
    rlang::abort("`separate.tbl_spark` requires Spark 2.4.0 or higher")
  }

  var <- tidyselect::vars_pull(colnames(data), !!rlang::enquo(col))

  col <- quote_sql_name(var)
  if (is.numeric(sep)) {
    sql <- strsep_to_sql(col, into, sep)
    out <- data %>>% dplyr::mutate %@% sql
  } else {
    substr_arr_col <- random_string("__tidyr_separate_tmp_")
    n <- length(into)
    split_str_sql <- str_split_fixed_to_sql(
      substr_arr_col = substr_arr_col,
      col = col,
      sep = sep,
      n = n,
      extra = extra,
      fill = fill,
      impl = if (spark_version(sc) >= "3.0.0") "SPLIT" else "SPARKLYR_STR_SPLIT"
    )
    substr_arr_col <- quote_sql_name(substr_arr_col)
    fill_left <- identical(fill, "left")
    assign_results_sql <- lapply(
      seq_along(into),
      function(idx) {
        dplyr::sql(
          if (fill_left) {
            sprintf(
              "IF(%s, NULL, %s)",
              sprintf("%d <= %d - SIZE(%s)", idx, n, substr_arr_col),
              sprintf(
                "ELEMENT_AT(%s, %d - ARRAY_MAX(ARRAY(0, %d - SIZE(%s))))",
                substr_arr_col,
                idx,
                n,
                substr_arr_col
              )
            )
          } else {
            sprintf(
              "IF(%d <= SIZE(%s), ELEMENT_AT(%s, %d), NULL)",
              idx,
              substr_arr_col,
              substr_arr_col,
              idx
            )
          }
        )
      }
    )
    names(assign_results_sql) <- into
    assign_results_sql <- assign_results_sql[!is.na(into)]
    out <- data %>>%
      dplyr::mutate %@% split_str_sql %>%
      process_warnings(substr_arr_col, n, extra, fill) %>>%
      dplyr::mutate %@% assign_results_sql
  }

  preserved <- setdiff(dplyr::group_vars(data), into)
  preserved <- setdiff(preserved, var)
  output_cols <- if (remove) into else union(colnames(data), into)
  output_cols <- union(preserved, output_cols)
  output_cols <- output_cols %>% lapply(as.symbol)

  update_group_vars(data, out, preserved) %>>%
    dplyr::select %@% output_cols
}
