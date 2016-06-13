#' @import yaml
spark_config_build <- function(master, config = NULL) {
  baseConfig <- list()

  if (spark_master_is_local(master)) {
    localConfigFile <- system.file(file.path("conf", "config-template.yml"), package = "rspark")

    packageConfig <- yaml::yaml.load_file(localConfigFile)
    baseConfig <- modifyList(packageConfig$default$rspark, packageConfig$local$rspark)
  }

  userConfig <- list()
  tryCatch(function() {
    userConfig <- config::get("rspark")
  }, error = function(e) {
  })

  mergedConfig <- modifyList(baseConfig, userConfig)

  # Give preference to config settings passed directly through spark_connect
  if (!is.null(config)) {
    mergedConfig <- modifyList(mergedConfig, config)
  }

  mergedConfig
}
