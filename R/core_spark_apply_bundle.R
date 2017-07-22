core_spark_apply_bundle_path <- function() {
  file.path(tempdir(), "packages.tar")
}

#' Creates a bundle of dependencies required by \code{spark_apply()}
#'
#' @keywords internal
#' @export
core_spark_apply_bundle <- function() {
  userPackages <- .libPaths()[[1]]
  packagesTar <- core_spark_apply_bundle_path()

  if (!file.exists(packagesTar)) {
    system2("tar", c("-cf", packagesTar, "-C", userPackages[[1]], "."))
  }

  packagesTar
}

core_spark_apply_unbundle_path <- function() {
  file.path("sparklyr-bundle")
}

#' Extracts a bundle of dependencies required by \code{spark_apply()}
#'
#' @param bundle_path Path to the bundle created using \code{core_spark_apply_bundle()}
#' @param base_path Base path to use while extracting bundles
#'
#' @keywords internal
#' @export
core_spark_apply_unbundle <- function(bundle_path, base_path) {
  extractPath <- file.path(base_path, core_spark_apply_unbundle_path())

  if (!dir.exists(extractPath)) dir.create(extractPath, recursive = TRUE)

  system2("tar", c("-xf", bundle_path, "-C", extractPath))

  extractPath
}
