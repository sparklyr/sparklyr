
#' Find path to Java
#'
#' Finds the path to \code{JAVA_HOME}.
#'
#' @param throws Throw an error when path not found?
#'
#' @export
#' @keywords internal
spark_get_java <- function(throws = FALSE) {
  java_home <- Sys.getenv("JAVA_HOME", unset = NA)
  if (!is.na(java_home)) {
    java <- file.path(java_home, "bin", "java")
    if (identical(.Platform$OS.type, "windows")) {
      java <- paste0(java, ".exe")
    }
    if (!file.exists(java)) {
      if (throws) {
        stop(
          "Java is required to connect to Spark. ",
          "JAVA_HOME is set to '",
          java_home,
          "' but does not point to a valid version. ",
          "Please fix JAVA_HOME or reinstall from: ",
          java_install_url()
        )
      }
      java <- ""
    }
  } else {
    java <- Sys.which("java")
  }
  java
}

validate_java_version <- function(master, spark_home) {
  # if someone sets SPARK_HOME and we are not in local more, assume Java
  # is available since some systems.
  # (e.g. CDH) use versions of java not discoverable through JAVA_HOME.
  if (!spark_master_is_local(master) && !is.null(spark_home) && nchar(spark_home) > 0) {
    return(TRUE)
  }

  # find the active java executable
  java <- spark_get_java(throws = TRUE)
  if (!nzchar(java)) {
    stop(
      "Java is required to connect to Spark. Please download and install Java from ",
      java_install_url()
    )
  }

  # query its version
  version <- system2(java, "-version", stderr = TRUE, stdout = TRUE)
  java_version <- validate_java_version_line(master, version)

  spark_version <- spark_version_from_home(spark_home)
  if (compareVersion(java_version, "11") >= 0 && compareVersion(spark_version, "3.0.0") < 0) {
    stop("Java 11 is only supported for Spark 3.0.0+", call. = FALSE)
  }

  TRUE
}

java_is_x64 <- function() {
  java <- spark_get_java(throws = TRUE)
  if (!nzchar(java)) {
    return(FALSE)
  }

  version <- system2(java, "-version", stderr = TRUE, stdout = TRUE)
  any(grepl("64-Bit", version))
}

java_install_url <- function() {
  "https://www.java.com/en/"
}

validate_java_version_line <- function(master, version) {
  if (length(version) < 1) {
    stop(
      "Java version not detected. Please download and install Java from ",
      java_install_url()
    )
  }

  # find line with version info
  versionLine <- version[grepl("version", version)]
  if (length(versionLine) != 1) {
    stop("Java version detected but couldn't parse version from ", paste(version, collapse = " - "))
  }

  splatVersion <- if (grepl("openjdk version", versionLine)) {
    strsplit(versionLine, "\"")[[1]][[2]]
  } else {
    splat <- strsplit(versionLine, "\\s+", perl = TRUE)[[1]]
    #Getting rid of dates when present before parsing version from 'java -version'
    splat <- splat[!grepl("[0-9]{4}-[0-9]{2}-[0-9]{2}", splat)]
    splat[grepl("[0-9]{1,2}(\\.[0-9]+\\.[0-9]+)?", splat)]
  }

  if (length(splatVersion) != 1) {
    stop("Java version detected but couldn't parse version from: ", versionLine)
  }

  parsedVersion <- regex_replace(
    splatVersion,
    "^\"|\"$" = "",
    "_" = ".",
    "[^0-9.]+" = ""
  )

  if (!is.character(parsedVersion) || nchar(parsedVersion) < 1) {
    stop("Java version detected but couldn't parse version from: ", versionLine)
  }

  # ensure Java 1.7 or higher
  if (compareVersion(parsedVersion, "1.7") == -1) {
    stop(
      "Java version", parsedVersion, " detected but 1.7+ is required. Please download and install Java from ",
      java_install_url()
    )
  }

  if (compareVersion(parsedVersion, "1.9") >= 0 &&
    compareVersion(parsedVersion, "11") == -1 &&
    spark_master_is_local(master) && !getOption("sparklyr.java9", FALSE)) {
    stop(
      "Java 9 is currently unsupported in Spark distributions unless you manually install Hadoop 2.8 ",
      "and manually configure Spark. Please consider uninstalling Java 9 and reinstalling Java 8. ",
      "To override this failure set 'options(sparklyr.java9 = TRUE)'."
    )
  }

  parsedVersion
}
