#' Defines a configuration file based on the config package and built-in defaults
#' @export
#' @param file Name of the configuration file
#' @param use_default TRUE to use the built-in detaults provided in this package
spark_config <- function(file = "config.yml", use_default = TRUE) {
  baseConfig <- list()

  if (use_default) {
    localConfigFile <- system.file(file.path("conf", "config-template.yml"), package = "sparklyr")
    baseConfig <- config::get(file = localConfigFile)
  }

  userConfig <- tryCatch(config::get(file = file), error = function(e) NULL)

  mergedConfig <- merge_lists(baseConfig, userConfig)
  mergedConfig
}

spark_config_params <- function(config, isLocal, pattern) {
  configNames <- Filter(function(e) {
    found <- substring(e, 1, nchar(pattern)) == pattern

    if (grepl("\\.local$", e) && !isLocal)
      found <- FALSE

    if (grepl("\\.remote$", e) && isLocal)
      found <- FALSE

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

# recursively merge two lists (extracted from code used by rmarkdown
# package to merge _output.yml, _site.yml, front matter, etc.:
# https://github.com/rstudio/rmarkdown/blob/master/R/util.R#L174)
merge_lists <- function (base_list, overlay_list, recursive = TRUE) {
  if (length(base_list) == 0)
    overlay_list
  else if (length(overlay_list) == 0)
    base_list
  else {
    merged_list <- base_list
    for (name in names(overlay_list)) {
      base <- base_list[[name]]
      overlay <- overlay_list[[name]]
      if (is.list(base) && is.list(overlay) && recursive)
        merged_list[[name]] <- merge_lists(base, overlay)
      else {
        merged_list[[name]] <- NULL
        merged_list <- append(merged_list,
                              overlay_list[which(names(overlay_list) %in% name)])
      }
    }
    merged_list
  }
}


