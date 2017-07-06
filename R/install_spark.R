#' @import sparkinstall

spark_install_find <- function(sparkVersion = NULL,
                               hadoopVersion = NULL,
                               installedOnly = TRUE,
                               latest = FALSE,
                               connecting = FALSE) {
  sparkinstall::spark_install_find(version = sparkVersion,
                                   hadoop_version = hadoopVersion,
                                   installed_only = installedOnly,
                                   latest = latest,
                                   hint = connecting,
                                   paths = spark_install_old_dir())
}

#' determine the version that will be used by default if version is NULL
#' @export
#' @keywords internal
spark_default_version <- function() {
  sparkinstall::spark_default_version(paths = spark_install_old_dir())
}

# Do not remove (it's used by the RStudio IDE)
spark_home <- function() {
  home <- Sys.getenv("SPARK_HOME", unset = NA)
  if (is.na(home))
    home <- NULL
  home
}

#' Download and install various versions of Spark
#'
#' Install versions of Spark for use with local Spark connections
#'   (i.e. \code{spark_connect(master = "local"})
#'
#' @param version Version of Spark to install. See \code{spark_available_versions} for a list of supported versions
#' @param hadoop_version Version of Hadoop to install. See \code{spark_available_versions} for a list of supported versions
#' @param reset Attempts to reset settings to defaults.
#' @param logging Logging level to configure install. Supported options: "WARN", "INFO"
#' @param verbose Report information as Spark is downloaded / installed
#' @param tarfile Path to TAR file conforming to the pattern spark-###-bin-(hadoop)?### where ###
#' reference spark and hadoop versions respectively.
#'
#' @return List with information about the installed version.
#'
#' @export
spark_install <- function(version = NULL,
                          hadoop_version = NULL,
                          reset = TRUE,
                          logging = "INFO",
                          verbose = interactive())
{
  sparkinstall::spark_install(version = version,
                              hadoop_version = hadoop_version,
                              reset = reset,
                              logging = logging,
                              verbose = verbose,
                              paths = spark_install_old_dir())
}

#' @rdname spark_install
#' @export
spark_uninstall <- function(version, hadoop_version) {
  sparkinstall::spark_uninstall(version = version,
                                hadoop_version = hadoop_version,
                                paths = spark_install_old_dir())
}

spark_install_old_dir <- function() {
  getOption("spark.install.dir", rappdirs::app_dir("spark", "rstudio")$cache())
}

#' @rdname spark_install
#' @export
spark_install_dir <- function() {
  sparkinstall::spark_install_dir()
}


#' @rdname spark_install
#' @export
spark_install_tar <- function(tarfile) {
  sparkinstall::spark_install_tar(tarfile)
}

#' @rdname spark_install
#' @export
spark_installed_versions <- function() {
  sparkinstall::spark_installed_versions(paths = spark_install_old_dir())
}

#' @rdname spark_install
#' @export
spark_available_versions <- function() {
  sparkinstall::spark_available_versions()
}
