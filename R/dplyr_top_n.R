# Workaround for https://github.com/tidyverse/dplyr/issues/2848
# Reverts back to commit https://raw.githubusercontent.com/tidyverse/dplyr/f188ff2654e2f2afab5ddbc8f4cbb5f4ff9df5be/R/top-n.R
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
