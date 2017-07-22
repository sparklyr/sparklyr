spark_apply_package <- function() {
  userPackages <- Filter(function(e) grepl("home", e), .libPaths())
  if (length(userPackages) == 0) NULL
  else {
    packagesTar <- file.path(tempdir(), "packages.tar")
    if (!file.exists(packagesTar)) {
      system2("tar", c("-cf", packagesTar, "-C", userPackages[[1]], "."))
    }

    packagesTar
  }
}
