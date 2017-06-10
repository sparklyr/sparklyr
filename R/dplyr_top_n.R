#' Select top (or bottom) n rows (by value)
#'
#' This is a convenient wrapper that uses [filter()] and
#' [min_rank()] to select the top or bottom entries in each group,
#' ordered by `wt`.
#'
#' @param x a [tbl()] to filter
#' @param n number of rows to return. If `x` is grouped, this is the
#'   number of rows per group. Will include more than `n` rows if
#'   there are ties.
#'
#'   If `n` is positive, selects the top `n` rows. If negative,
#'   selects the bottom `n` rows.
#' @param wt (Optional). The variable to use for ordering. If not
#'   specified, defaults to the last variable in the tbl.
#'
#'   This argument is automatically [quoted][rlang::quo] and later
#'   [evaluated][rlang::eval_tidy] in the context of the data
#'   frame. It supports [unquoting][rlang::quasiquotation]. See
#'   `vignette("programming")` for an introduction to these concepts.
#' @export
#' @importFrom rlang enquo
#' @importFrom rlang quo_is_missing
#' @importFrom rlang new_quosure
#' @importFrom rlang quo_is_symbol
#' @importFrom rlang quo
#' @importFrom rlang eval_tidy
#' @importFrom rlang inform
#' @importFrom rlang is_scalar_integerish
#' @importFrom rlang sym
top_n <- function(x, n, wt) {
  # Workaround for https://github.com/tidyverse/dplyr/issues/2848
  # Reverts back to commit https://raw.githubusercontent.com/tidyverse/dplyr/f188ff2654e2f2afab5ddbc8f4cbb5f4ff9df5be/R/top-n.R

  wt <- enquo(wt)

  if (quo_is_missing(wt)) {
    vars <- tbl_vars(x)
    inform(paste("Selecting by ", vars[length(vars)]))
    wt <- new_quosure(sym(vars[length(vars)]))
  }

  stopifnot(is_scalar_integerish(n))
  if (n > 0) {
    quo <- quo(filter(x, min_rank(desc(!!wt)) <= !!n))
  } else {
    quo <- quo(filter(x, min_rank(!!wt) <= !!abs(n)))
  }

  eval_tidy(quo)
}

overwrite_dplyr_top_n <- function(...) {
  if ("dplyr" %in% loadedNamespaces()) {
    ns <- asNamespace("dplyr")
    unlock <- get("unlockBinding")
    unlock("top_n", ns)
    assign("top_n", top_n, envir = ns)
    lock <- get("lockBinding")
    lock("top_n", env = ns)
  }
}
