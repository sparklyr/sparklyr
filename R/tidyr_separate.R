#' @include tidyr_utils.R
NULL

validate_args <- function(into, sep) {
  if (!is.character(into)) {
    rlang::abort("`into` must be a character vector")
  }

  if (!is.numeric(sep) && !rlang::is_character(sep)) {
    rlang::abort("`sep` must be either numeric or character")
  }

  if (is.numeric(sep) && !identical(length(sep) + 1L, length(into))) {
    rlang::abort("The length of `sep` should be one less than `into`.")
  }
}

#' @importFrom tidyr separate
#' @export
separate.tbl_spark <- function(data, col, into, sep = "[^[:alnum:]]+",
                               remove = TRUE, convert = FALSE, extra = "warn", fill = "warn", ...) {
  if (!identical(convert, FALSE)) {
    rlang::warn("`convert` will be ignored for Spark dataframes!")
  }
  check_present(col)
  if (rlang::is_character(sep)) {
    sep <- pcre_to_java(sep)
  }
  validate_args(into, sep)

  sc <- spark_connection(data)
  if (spark_version(sc) < "2.4.0") {
    rlang::abort("`separate.tbl_spark` requires Spark 2.4.0 or higher")
  }

  var <- tidyselect::vars_pull(colnames(data), !!rlang::enquo(col))

  out <- str_separate(data, quote_sql_name(var), into, sep, extra, fill)
  preserved <- setdiff(dplyr::group_vars(data), into)
  preserved <- setdiff(preserved, var)
  output_cols <- if (remove) into else union(colnames(data), into)
  output_cols <- union(preserved, output_cols)
  output_cols <- output_cols %>% lapply(as.symbol)

  update_group_vars(data, out, preserved) %>>%
    dplyr::select %@% output_cols
}
