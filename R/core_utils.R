core_get_package_function <- function(packageName, functionName) {
  if (packageName %in% rownames(installed.packages()) &&
      exists(functionName, envir = asNamespace(packageName)))
    get(functionName, envir = asNamespace(packageName))
  else
    NULL
}

core_remove_factors <- function(df) {
  if (any(sapply(df, is.factor))) {
    df <- as.data.frame(lapply(df, function(x) if(is.factor(x)) as.character(x) else x), stringsAsFactors = FALSE)
  }

  df
}
