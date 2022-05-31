# nocov start

#' Install Livy
#'
#' Automatically download and install \href{https://livy.apache.org/}{\samp{livy}}.
#' \samp{livy} provides a REST API to Spark.
#'
#' @param version The version of \samp{livy} to be installed.
#' @param spark_home The path to a Spark installation. The downloaded and
#'   installed version of \samp{livy} will then be associated with this Spark
#'   installation. When unset (\samp{NULL}), the value is inferred based on
#'   the value of \samp{spark_version} supplied.
#' @param spark_version The version of Spark to use. When unset (\samp{NULL}),
#'   the value is inferred based on the value of \samp{livy_version} supplied.
#'   A version of Spark known to be compatible with the requested version of
#'   \samp{livy} is chosen when possible.
#' @export
livy_install <- function(version = "0.6.0",
                         spark_home = NULL,
                         spark_version = NULL) {
  version <- cast_string(version)

  # determine an appropriate spark version
  if (is.null(spark_version)) {

    # if spark_home is set, then infer spark version based on that
    if (!is.null(spark_home)) {
      spark_version <- spark_version_from_home(spark_home, default = spark_version)
    } else {
      spark_version <- switch(
        version,
        "0.2.0" = "1.6.2",
        "0.3.0" = "2.0.1",
        "0.4.0" = "2.1.0",
        "0.5.0" = "2.2.0",
        "0.6.0" = "2.4.4"
      )
    }

    if (interactive()) {
      message("* Using Spark: ", spark_version)
    }
  }

  # warn if the user attempts to use livy 0.2.0 with Spark >= 2.0.0
  spark_version <- cast_string(spark_version)
  if (version == "0.2.0" &&
    numeric_version(spark_version) >= "2.0.0") {
    stopf("livy %s is not compatible with Spark (>= %s)", version, "2.0.0")
  }

  # determine spark home (auto-install as needed)
  if (is.null(spark_home)) {
    spark_home <- spark_home_dir(version = spark_version)
    if (is.null(spark_home)) {
      prompt <- sprintf(
        "* Spark %s is not installed. Download and install? [Y/n]: ",
        spark_version
      )

      response <- readline(prompt = prompt)
      if (!identical(tolower(response[1]), "y")) {
        stop("Installation aborted by user.")
      }

      spark_install(version = spark_version)
      spark_home <- spark_home_dir(version = spark_version)
    }

    if (interactive()) {
      message("* Using Spark home: ", spark_home)
    }
  }

  # ensure that spark home exists
  if (!file.exists(spark_home)) {
    stopf("No Spark installation found at '%s'", spark_home)
  }

  # construct path where livy will be unpacked
  livy_cache <- livy_install_dir()
  livy_path <- file.path(
    livy_cache,
    sprintf("livy-%s", version, basename(spark_home))
  )

  # if livy is already installed, bail
  if (file.exists(livy_path)) {
    message("* '", basename(livy_path), "' already installed")
    return(invisible(livy_path))
  }

  # construct path to livy download
  if (version <= "0.3.0") {
    url <- sprintf(
      "http://archive.cloudera.com/beta/livy/livy-server-%s.zip",
      version
    )
  } else {
    apache_prefix <- if (package_version(version) >= package_version("0.6.0")) "apache-" else ""
    url <- sprintf(
      "http://archive.apache.org/dist/incubator/livy/%s-incubating/%slivy-%s-incubating-bin.zip",
      version,
      apache_prefix,
      version
    )
  }

  # download to cache directory
  ensure_directory(livy_cache)
  destfile <- file.path(livy_cache, basename(url))
  on.exit(unlink(destfile), add = TRUE)

  if (interactive()) {
    message("* Installing livy ", version)
    message("* Downloading from:\n- ", shQuote(url))
    message("* Installing to:\n- ", shQuote(aliased_path(livy_path)))
  }

  download_file(url, destfile = destfile)
  if (!file.exists(destfile)) {
    stop("livy download failed")
  }

  # unzip to livy directory (not full path; we will rename after)
  extracted_files <- unzip(destfile, exdir = livy_cache)

  # determine the extracted folder's name
  relative_paths <- substring(extracted_files, nchar(livy_cache) + 2L)
  first_slashes <- as.integer(regexec("/", relative_paths, fixed = TRUE))
  folder_name <- unique(substring(relative_paths, 1L, first_slashes - 1L))
  if (length(folder_name) != 1) {
    stop("failed to ascertain unpacked livy folder name")
  }

  # rename folder to taret location
  file.rename(file.path(livy_cache, folder_name), livy_path)
  if (!file.exists(livy_path)) {
    stopf("failed to move '%s' to '%s'", folder_name, basename(livy_path))
  }

  livyStart <- file.path(livy_path, "bin/livy-server")
  livyLogs <- file.path(livy_path, "logs")

  if (!dir.exists(livyLogs)) {
    dir.create(livyLogs)
  }

  if (.Platform$OS.type == "unix") {
    system2("chmod", c("744", livyStart))
  }

  # return installation path on success
  if (interactive()) {
    message("* livy ", version, " installed successfully!")
  }

  invisible(livy_path)
}

#' @rdname livy_install
#' @export
livy_available_versions <- function() {
  versions <- data.frame(livy = c("0.2.0", "0.3.0", "0.4.0", "0.5.0", "0.6.0"))

  versions
}

livy_versions_file_pattern <- function() {
  "livy-(.*)"
}

#' @rdname livy_install
#' @export
livy_install_dir <- function() {
  normalizePath(
    getOption("livy.install.dir", rappdirs::app_dir("livy", "rstudio")$cache())
  )
}

#' @rdname livy_install
#' @export
livy_installed_versions <- function() {
  livyDir <- livy_install_dir()

  livy <- character()
  dir <- character()
  livyVersionDir <- character()

  lapply(dir(livy_install_dir(), full.names = TRUE), function(maybeDir) {
    if (dir.exists(maybeDir)) {
      fileName <- basename(maybeDir)
      m <- regmatches(fileName, regexec(livy_versions_file_pattern(), fileName))[[1]]
      if (length(m) > 1) {
        livy <<- c(livy, m[[2]])
        dir <<- c(dir, basename(maybeDir))
        livyVersionDir <<- c(livyVersionDir, maybeDir)
      }
    }
  })

  versions <- data.frame(
    livy = livy,
    livyVersionDir = livyVersionDir,
    stringsAsFactors = FALSE
  )

  versions
}

livy_install_find <- function(livyVersion = NULL) {
  versions <- livy_installed_versions()
  versions <- if (is.null(livyVersion)) versions else versions[versions$livy == livyVersion, ]

  if (NROW(versions) == 0) {
    livyInstall <- quote(livy_install(version = ""))
    livyInstall$version <- livyVersion

    stop("Livy version not installed. To install, use ", deparse(livyInstall))
  }

  tail(versions, n = 1)
}

#' Find the LIVY_HOME directory for a version of Livy
#'
#' Find the LIVY_HOME directory for a given version of Livy that
#' was previously installed using \code{\link{livy_install}}.
#'
#' @param version Version of Livy
#'
#' @return Path to LIVY_HOME (or \code{NULL} if the specified version
#'   was not found).
#'
#' @keywords internal
#'
#' @rdname livy_install
#' @export
livy_home_dir <- function(version = NULL) {
  tryCatch(
    {
      installInfo <- livy_install_find(livyVersion = version)
      installInfo$livyVersionDir
    },
    error = function(e) {
      NULL
    }
  )
}

# nocov end
