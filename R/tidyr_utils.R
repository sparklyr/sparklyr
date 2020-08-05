ungroup <- function(x, ungrouped_vars) {
  if (length(ungrouped_vars) > 0) {
    args <- list(x) %>% append(lapply(ungrouped_vars, as.symbol))
    do.call(dplyr::ungroup, args)
  } else {
    x
  }
}
