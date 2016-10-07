#' Install Livy
#'
#' Automatically download and install \href{http://livy.io/}{\samp{livy}}.
#' \samp{livy} provides a REST API to Spark.
#'
#' @param spark_home The path to a Spark installation. The downloaded and
#'   installed version of \samp{livy} will then be associated with this Spark
#'   installation. When unset (\samp{NULL}), the value is inferred based on
#'   the value of \samp{spark_version} supplied.
#' @param spark_version The version of Spark to use. When unset (\samp{NULL}),
#'   the value is inferred based on the value of \samp{livy_version} supplied.
#'   A version of Spark known to be compatible with the requested version of
#'   \samp{livy} is chosen when possible.
#' @param livy_version The version of \samp{livy} to be installed.
#' @export
livy_install <- function(spark_home    = NULL,
                         spark_version = NULL,
                         livy_version  = "0.2.0")
{
  # determine an appropriate spark version
  spark_version <- spark_version %||% switch(
    livy_version,
    "0.2.0" = "1.6.2",
    "0.3.0" = "2.0.1"
  )

  # determine spark home
  spark_home <- spark_home %||% spark_home_dir(version = spark_version)

  # ensure that spark home exists
  if (!file.exists(spark_home)) {

    if (!interactive())
      stopf("No Spark installation found at '%s'", spark_home)

    prompt <- sprintf(
      "Spark %s is not installed. Download and install? [Y/n]: ",
      spark_version
    )

    response <- readline(prompt = prompt)
    if (!identical(tolower(response[1]), "y"))
      stop("Installation aborted by user.")
  }

  # use the directory path as the version
  spark_version <- basename(spark_home)

  # construct path where livy will be unpacked
  livy_cache <- rappdirs::app_dir("livy", "rstudio")$cache()
  livy_path <- file.path(
    livy_cache,
    sprintf("livy-%s-%s", livy_version, spark_version)
  )

  # if livy is already installed, bail
  if (file.exists(livy_path)) {
    message("* '", basename(livy_path), "' already installed")
    return(invisible(livy_path))
  }

  # construct path to livy download
  url <- sprintf(
    "http://archive.cloudera.com/beta/livy/livy-server-%s.zip",
    livy_version
  )

  # download to cache directory
  ensure_directory(livy_cache)
  destfile <- file.path(livy_cache, basename(url))
  on.exit(unlink(destfile), add = TRUE)
  download.file(url, destfile = destfile)
  if (!file.exists(destfile))
    stop("Livy download failed")

  # unzip to livy directory (not full path; we will rename after)
  extracted_files <- unzip(destfile, exdir = livy_cache)

  # determine the extracted folder's name
  relative_paths <- substring(extracted_files, nchar(livy_cache) + 2L)
  first_slashes <- as.integer(regexec("/", relative_paths, fixed = TRUE))
  folder_name <- unique(substring(relative_paths, 1L, first_slashes - 1L))
  if (length(folder_name) != 1)
    stop("failed to ascertain unpacked livy folder name")

  # rename folder to taret location
  file.rename(file.path(livy_cache, folder_name), livy_path)
  if (!file.exists(livy_path))
    stopf("failed to move '%s' to '%s'", folder_name, basename(livy_path))

  # return installation path on success
  invisible(livy_path)
}
