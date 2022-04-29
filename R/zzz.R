set_option_default <- function(...) {
  enumerate(list(...), function(key, value) {
    option <- getOption(key)
    if (is.null(option)) {
      do.call(base::options, stats::setNames(list(value), key))
    }
  })
}

.onLoad <- function(...) {
  if (!dbplyr_uses_ops()) {
    vctrs::s3_register("dbplyr::simulate_vars", "tbl_spark")
    vctrs::s3_register("dbplyr::simulate_vars_is_typed", "tbl_spark")
  }
}

utils::globalVariables(".")
