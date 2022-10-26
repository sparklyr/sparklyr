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

  .data <- set_simulate_temp(.data, ...)

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

  .data <- set_simulate_temp(.data, ...)

  if (identical(dots, dots_org)) {
    NextMethod()
  } else {
    mutate(.data, !!!dots)
  }
}

#' @export
#' @importFrom dplyr filter
filter.tbl_spark <- function(.data, ..., .preserve = FALSE) {
  .data <- reset_simulate_temp(.data, ...)
  if (!identical(.preserve, FALSE)) {
    stop("`.preserve` is not supported on database backends", call. = FALSE)
  }
  NextMethod()
}

#' @export
#' @importFrom dplyr select
select.tbl_spark <- function(.data, ...) {
  .data <- reset_simulate_temp(.data, ...)
  NextMethod()
}

#' @export
#' @importFrom dplyr summarise
#' @importFrom dbplyr op_vars
summarise.tbl_spark <- function(.data, ..., .groups = NULL) {
  .data <- reset_simulate_temp(.data, ...)
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

set_simulate_temp <- function(.data, ...) {
  dots_org <- rlang::enquos(...)
  if(dots_all_same(dots_org)) {
    .data$simulate_temp_schema<- run_simulate_vars(.data, FALSE)
  } else {
    .data$simulate_temp_schema <- NULL
  }
  .data
}

reset_simulate_temp <- function(.data, ...) {
  .data$simulate_temp_schema <-  NULL
  .data
}

dots_all_same <- function(dots) {
  dots_expr <- map(dots, rlang::quo_get_expr)
  ret <- purrr::map_lgl(
    seq_along(dots_expr),
    ~ dots_expr[[1]] == dots_expr[[.x]]
  ) %>%
    all()
  if(is.na(ret)) ret <- FALSE
  if(is.null(ret)) ret <- FALSE
  ret
}

