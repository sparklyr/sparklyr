#' @include dplyr_hof.R
#' @include spark_sql.R
#' @include utils.R
NULL

#' @export
#' @importFrom dplyr transmute
#' @importFrom dplyr all_of
transmute.tbl_spark <- function(.data, ...) {
  dots_org <- rlang::enquos(..., .named = TRUE)
  dots <- fix_na_real_values(dots_org)
  if (identical(dots, dots_org)) {
    NextMethod()
  } else {
    transmute(.data, !!!dots)
  }
}

#' @export
#' @importFrom dplyr mutate
mutate.tbl_spark <- function(.data, ...) {
  dots_org <- rlang::enquos(...)
  dots <- fix_na_real_values(dots_org)
  if (identical(dots, dots_org)) {
    NextMethod()
  } else {
    mutate(.data, !!!dots)
  }
}

#' @export
#' @importFrom dplyr filter
filter.tbl_spark <- function(.data, ..., .preserve = FALSE) {
  if (!identical(.preserve, FALSE)) {
    stop("`.preserve` is not supported on database backends", call. = FALSE)
  }
  NextMethod()
}

#' @export
#' @importFrom dplyr select
select.tbl_spark <- function(.data, ...) {
    NextMethod()
}

#' @export
#' @importFrom dplyr summarise
#' @importFrom dbplyr op_vars
summarise.tbl_spark <- function(.data, ..., .groups = NULL) {
    NextMethod()
}

fix_na_real_values <- function(dots) {
  for (i in seq_along(dots)) {
    if (identical(rlang::quo_get_expr(dots[[i]]), rlang::expr(NA_real_))) {
      dots[[i]] <- rlang::quo(dbplyr::sql("CAST(NULL AS DOUBLE)"))
    }
  }

  dots
}
