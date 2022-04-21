set_option_default <- function(...) {
  enumerate(list(...), function(key, value) {
    option <- getOption(key)
    if (is.null(option)) {
      do.call(base::options, stats::setNames(list(value), key))
    }
  })

  if (!dbplyr_uses_ops()) {
    vctrs::s3_register("dbplyr::simulate_vars", "tbl_spark")
  }
}

utils::globalVariables(".")
