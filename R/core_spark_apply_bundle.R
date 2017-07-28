core_spark_apply_bundle_path <- function() {
  file.path(tempdir(), "packages.tar")
}

#' Creates a bundle of dependencies required by \code{spark_apply()}
#'
#' @keywords internal
#' @export
core_spark_apply_bundle <- function() {
  packagesTar <- core_spark_apply_bundle_path()

  args <- c("-cf", packagesTar)
  lapply(.libPaths(), function(e) {
    args <<- c(args, "-C", e)
    args <<- c(args, ".")
  })

  if (!file.exists(packagesTar)) {
    system2("tar", args)
  }

  packagesTar
}

core_spark_apply_unbundle_path <- function() {
  file.path("sparklyr-bundle")
}
