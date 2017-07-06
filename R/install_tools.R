#' helper function to sync sparkinstall project to sparklyr
#'
#' @param project_path The path to the sparkinstall project
spark_install_sync <- function(project_path) {
  sources <- c(
    "install.R",
    "versions.R",
    "windows.R"
  )

  destinations <- c(
    "install_spark.R",
    "install_spark_versions.R",
    "install_spark_windows.R"
  )

  sources_paths <- file.path("R", sources)
  destinations_paths <- file.path("R", destinations)

  file.copy(
    from = file.path(project_path, sources_paths),
    to = file.path(getwd(), destinations_paths),
    overwrite = TRUE
  )
}
