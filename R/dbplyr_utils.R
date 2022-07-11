#' @include utils.R
NULL

# NOTE: everything here is entirely copy-pasted from dbplyr.
# Reason for doing so is `sparklyr` needs to override some other
# implementation detail of `dplyr::mutate` so that it can correctly handle
# the `mutate(across(where(...), ...))` type of usage for Spark dataframes

nest_vars <- function(.data, dots, all_vars) {
  stop("No support")
}

all_names <- function(x) {

  if (is.name(x)) return(as.character(x))

  if (rlang::is_quosure(x)) return(all_names(rlang::quo_get_expr(x)))

  if (!is.call(x)) return(NULL)

  unique(unlist(lapply(x[-1], all_names), use.names = FALSE))
}

dbplyr_uses_ops <- function() {
  older_dbplyr <- packageVersion("dbplyr") <= package_version("2.1.1")
  if(older_dbplyr) stop("No support")
  older_dbplyr
}
