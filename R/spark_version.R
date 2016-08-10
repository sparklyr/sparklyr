spark_version_clean <- function(version) {
  gsub("([0-9]+\\.?)[^0-9\\.](.*)","\\1", version)
}

#' Version of Spark for a connection
#'
#' @param sc \code{spark_connection}
#'
#' @return A \code{\link{numeric_version}} object
#'
#' @export
spark_version <- function(sc) {
  # get the version
  version <- invoke(spark_context(sc), "version")

  # Get rid of -preview and other suffix variations
  version <- spark_version_clean(version)

  # return numeric version
  numeric_version(version)
}

spark_version_from_home_version <- function() {
  version <- Sys.getenv("SPARK_HOME_VERSION")
  if (nchar(version) <= 0) NULL else version
}

#' Version of Spark for a SPARK_HOME directory
#'
#' @param spark_home Path to SPARK_HOME
#' @param default The version to use as default
#'
#' @rdname spark_version
#'
#' @export
spark_version_from_home <- function(spark_home, default = NULL) {
  versionAttempts <- list(
    useReleaseFile = function() {
      versionedFile <- file.path(spark_home, "RELEASE")
      if (file.exists(versionedFile)) {
        releaseContents <- readLines(versionedFile)

        if (!is.null(releaseContents) && length(releaseContents) > 0) {
          gsub("Spark | built.*", "", releaseContents[[1]])
        }
      }
    },
    useAssemblies = function() {
      candidateVersions <- list(
        list(path = "lib", pattern = "spark-assembly-([0-9\\.]*)-hadoop.[0-9\\.]*\\.jar"),
        list(path = "yarn", pattern = "spark-([0-9\\.]*)-preview-yarn-shuffle\\.jar")
      )

      candidateFiles <- lapply(candidateVersions, function(e) {
        c(e,
          list(
            files = list.files(
              file.path(spark_home, e$path),
              pattern = e$pattern
            )
          )
        )
      })

      filteredCandidates <- Filter(function(f) length(f$files) > 0, candidateFiles)
      if (length(filteredCandidates) > 0) {
        valid <- filteredCandidates[[1]]
        e <- regexec(valid$pattern, valid$files[[1]])
        match <- regmatches(valid$files[[1]], e)
        if (length(match) > 0 && length(match[[1]]) > 1) {
          return(match[[1]][[2]])
        }
      }
    },
    useEnvironmentVariable = function() {
      spark_version_from_home_version()
    },
    useDefault = function() {
      default
    }
  )

  for (versionAttempt in versionAttempts) {
    result <- versionAttempt()
    if (length(result) > 0)
      return(spark_version_clean(result))
  }

  stop(
    "Failed to detect version from SPARK_HOME or SPARK_HOME_VERSION. ",
    "Try passing the spark version explicitly.")
}
