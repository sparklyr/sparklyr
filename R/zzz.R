set_option_default <- function(...) {
  enumerate(list(...), function(key, value) {
    option <- getOption(key)
    if (is.null(option))
      do.call(base::options, stats::setNames(list(value), key))
  })
}

.onLoad <- function(...) {
  register_dplyr_all()

  set_option_default(
    sparklyr.na.action.verbose = TRUE
  )

  overwrite_dplyr_top_n()
  setHook(packageEvent("dplyr", "attach"), function(...) {
    overwrite_dplyr_top_n()
  })
}
