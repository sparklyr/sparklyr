#' Read Spark Configuration
#'
#' @export
#' @param file Name of the configuration file
#' @param use_default TRUE to use the built-in defaults provided in this package for local deplyomnet - \href{https://github.com/rstudio/sparklyr/blob/master/inst/conf/config-template.yml}{see}
#' @param yarn TRUE to use the built-in defaults provided in this package for yarn cluster deplyomnet - \href{https://github.com/rstudio/sparklyr/blob/master/inst/conf/yarn-config-template.yml}{see}
#'
#' @details
#'
#' Read Spark configuration using the \pkg{\link[config]{config}} package.
#'
#' @return Named list with configuration data
spark_config <- function(file = "config.yml", use_default = TRUE, yarn = FALSE) {
  assert_that(is.logical(yarn), length(yarn) == 1)
  assert_that(is.logical(use_default), length(use_default) == 1)

  baseConfig <- list()

  if (use_default) {
    localConfigFile <- system.file(file.path("conf", "config-template.yml"), package = "sparklyr")
    baseConfig <- config::get(file = localConfigFile)
  }
  
  yarnConfig <- list()
  if (yarn) {
    yarnConfigFile <- system.file(file.path("conf", "yarn-config-template.yml"), package = "sparklyr")
    yarnConfig <- config::get(file = yarnConfigFile)
  }

  userConfig <- tryCatch(config::get(file = file), error = function(e) NULL)

  mergedConfig <- merge_lists(baseConfig, yarnConfig, userConfig)
  mergedConfig
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


