spark_apply_bundle_path <- function() {
  file.path(tempdir(), "packages")
}

spark_apply_bundle_file <- function(packages = TRUE) {
  file.path(
    spark_apply_bundle_path(),
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

#' Creates a bundle of dependencies required by \code{spark_apply()}
#'
#' @param packages List of packages to pack or \code{TRUE} to pack all.
#'
#' @keywords internal
#' @export
spark_apply_bundle <- function(packages = TRUE) {
  packagesTar <- spark_apply_bundle_file(packages)

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
        sublib_packages <- lapply(packages, function(p) {
          if (file.exists(file.path(e, p)) && !p %in% added_packages) {
            added_packages <<- c(added_packages, p)
            p
          }
        }) %>%
          (function(x) Filter(Negate(is.null), x)) %>%
          unlist()

        if (length(sublib_packages) > 0) c("-C", e, sublib_packages) else NULL
      }) %>% unlist()
    }
  )

  if (!file.exists(packagesTar)) {
    system2("tar", args)
  }

  packagesTar
}
