set_option_default <- function(...) {
  enumerate(list(...), function(key, value) {
    option <- getOption(key)
    if (is.null(option))
      do.call(base::options, stats::setNames(list(value), key))
  })
}

.onLoad <- function(...) {
  set_option_default(
    sparklyr.na.action.verbose = TRUE
  )
}

utils::globalVariables(".")
