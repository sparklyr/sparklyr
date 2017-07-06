#' helper function to sync sparkinstall project to sparklyr
#'
#' See: https://github.com/rstudio/spark-install
#'
#' @param project_path The path to the sparkinstall project
spark_install_sync <- function(project_path) {
  sources <- c(
    "R/install.R",
    "R/versions.R",
    "R/windows.R",
    "inst/extdata/config.json",
    "inst/extdata/versions.json"
  )

  destinations <- c(
    "R/install_spark.R",
    "R/install_spark_versions.R",
    "R/install_spark_windows.R",
    "inst/extdata/config.json",
    "inst/extdata/versions.json"
  )

  sources_paths <- sources
  destinations_paths <- destinations

  file.copy(
    from = file.path(project_path, sources_paths),
    to = file.path(getwd(), destinations_paths),
    overwrite = TRUE
  )
}
