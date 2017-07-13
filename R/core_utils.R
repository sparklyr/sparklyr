core_get_package_function <- function(packageName, functionName) {
  if (packageName %in% rownames(installed.packages()) &&
      exists(functionName, envir = asNamespace(packageName)))
    get(functionName, envir = asNamespace(packageName))
  else
    NULL
}
