#' Defines a configuration file based on the config package and built-in defaults
#' @export
#' @import yaml
#' @param file Name of the configuration file
#' @param use_default TRUE to use the built-in detaults provided in this package
spark_config <- function(file = "config.yml", use_default = TRUE) {
  baseConfig <- list()

  if (use_default) {
    localConfigFile <- system.file(file.path("conf", "config-template.yml"), package = "rspark")
    baseConfig <- config::get(file = localConfigFile)
  }

  userConfig <- list()
  tryCatch(function() {
    userConfig <- config::get(file = file)
  }, error = function(e) {
  })

  mergedConfig <- modifyList(baseConfig, userConfig)
  mergedConfig
}

spark_config_params <- function(config, isLocal, pattern) {
  configNames <- Filter(function(e) {
    found <- substring(e, 1, nchar(pattern)) == pattern

    if (grepl("\\.local$", configName) && !isLocal)
      found <- false

    if (grepl("\\.remote$", configName) && isLocal)
      found <- false

    found
  }, names(config))

  paramsNames <- lapply(configNames, function(configName) {
    paramName <- substr(configName, nchar(pattern) + 1, nchar(configName))
    paramName <- sub("(\\.local$)|(\\.remote$)", "", paramName, perl = TRUE)

    paramName
  })

  params <- lapply(configNames, function(configName) {
    config[[configName]]
  })

  names(params) <- paramsNames
  params
}
