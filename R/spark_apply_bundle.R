spark_apply_bundle_path <- function() {
  file.path(tempdir(), "packages")
}

spark_apply_bundle_file <- function(packages, base_path) {
  file.path(
    base_path,
    if (isTRUE(packages))
      "packages.tar"
    else
      paste(
        substr(
          digest::digest(
            paste(packages, collapse = "-"),
            algo = "sha256"
          ),
          start = 1,
          stop = 7
        ),
        "tar",
        sep = "."
      )
  )
}

#' Create Bundle for Spark Apply
#'
#' Creates a bundle of packages for \code{spark_apply()}.
#'
#' @param packages List of packages to pack or \code{TRUE} to pack all.
#' @param base_path Base path used to store the resulting bundle.
#'
#' @export
spark_apply_bundle <- function(packages = TRUE, base_path = getwd()) {
  packages <- if (is.character(packages)) spark_apply_packages(packages) else packages

  packagesTar <- spark_apply_bundle_file(packages, base_path)

  if (!dir.exists(spark_apply_bundle_path()))
    dir.create(spark_apply_bundle_path(), recursive = TRUE)

  args <- c(
    "-cf",
    packagesTar,
    if (isTRUE(packages)) {
      lapply(.libPaths(), function(e) {
        c("-C", e, ".")
      }) %>% unlist()
    } else {
      added_packages <- list()
      lapply(.libPaths(), function(e) {
        sublib_packages <- Filter(
          Negate(is.null),
          lapply(packages, function(p) {
            if (file.exists(file.path(e, p)) && !p %in% added_packages) {
              added_packages <<- c(added_packages, p)
              p
            }
          })) %>% unlist()

        if (length(sublib_packages) > 0) c("-C", e, sublib_packages) else NULL
      }) %>% unlist()
    }
  )

  if (!file.exists(packagesTar)) {
    system2("tar", args)
  }

  packagesTar
}
