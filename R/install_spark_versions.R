.globals <- new.env(parent = emptyenv())

spark_versions_file_pattern <- function() {
  "spark-(.*)-bin-(?:hadoop)?(.*)"
}

spark_versions_url <- function() {
  "https://raw.githubusercontent.com/rstudio/sparklyr/master/inst/extdata/install_spark.csv"
}

read_spark_versions_csv <- function(file = spark_versions_url()) {

  # see if we have a cached version
  if (!exists("sparkVersionsCsv", envir = .globals))
  {
    versionsCsv <- utils::read.csv(file,
                                   colClasses = c(hadoop = "character"),
                                   stringsAsFactors = FALSE)

    assign("sparkVersionsCsv", versionsCsv, envir = .globals)

  }

  .globals$sparkVersionsCsv
}


#' @rdname spark_install
#' @export
spark_installed_versions <- function() {

  spark <- character()
  hadoop <- character()
  dir <- character()
  lapply(dir(spark_install_dir(), full.names = TRUE), function(maybeDir) {
    if (dir.exists(maybeDir)) {
      fileName <- basename(maybeDir)
      m <- regmatches(fileName, regexec(spark_versions_file_pattern(), fileName))[[1]]
      if (length(m) > 2) {
        spark <<- c(spark, m[[2]])
        hadoop <<- c(hadoop, m[[3]])
        dir <<- c(dir, basename(maybeDir))
      }
    }
  })
  versions <- data.frame(spark = spark,
                         hadoop = hadoop,
                         dir = dir,
                         stringsAsFactors = FALSE)

  versions
}


#' @rdname spark_install
#' @export
spark_available_versions <- function() {
  versions <- read_spark_versions_csv()
  versions <- versions[versions$spark >= "1.6.0", 1:2]
  versions$install <- paste0("spark_install(version = \"",
                             versions$spark, "\", ",
                             "hadoop_version = \"", versions$hadoop,
                             "\")")
  versions
}


spark_versions <- function(latest = TRUE) {

  # NOTE: this function is called during configure and the 'sparklyr' package
  # will not be available at that time; allow overriding with environment variable
  packagePathEnv <- Sys.getenv("R_SPARKLYR_INSTALL_INFO_PATH", unset = NA)
  packagePath <- if (!is.na(packagePathEnv))
    packagePathEnv
  else
    system.file(file.path("extdata", "install_spark.csv"), package = "sparklyr")

  downloadData <- NULL
  if (latest) {
    tryCatch({
      suppressWarnings(
        downloadData <- read_spark_versions_csv()
      )
    }, error = function(e) {
    })
  }

  if (is.null(downloadData) || is.null(downloadData$spark)) {
    # warning("Failed to retrieve the latest download links")
    downloadData <- read_spark_versions_csv(packagePath)
  }


  downloadData$installed <- rep(FALSE, NROW(downloadData))

  mergedData <- downloadData
  lapply(
    Filter(function(e) !is.null(e),
      lapply(dir(spark_install_dir(), full.names = TRUE), function(maybeDir) {
        if (dir.exists(maybeDir)) {
          fileName <- basename(maybeDir)
          m <- regmatches(fileName, regexec(spark_versions_file_pattern(), fileName))[[1]]
          if (length(m) > 2) list(spark = m[[2]], hadoop = m[[3]]) else NULL
        }
      })
    ),
    function(row) {
      currentRow <- downloadData[downloadData$spark == row$spark & downloadData$hadoop == row$hadoop, ]
      notCurrentRow <- mergedData[mergedData$spark != row$spark | mergedData$hadoop != row$hadoop, ]

      newRow <- c(row, installed = TRUE)
      newRow$default <- if (NROW(currentRow) > 0) currentRow$default else FALSE
      newRow$download <- if (NROW(currentRow) > 0) currentRow$download else ""
      newRow$hadoop_label <- if (NROW(currentRow) > 0) currentRow$hadoop_label else paste("Hadoop", row$hadoop)
      newRow$hadoop_default <- if (NROW(currentRow) > 0) currentRow$hadoop_default else FALSE

      mergedData <<- rbind(notCurrentRow, newRow)
    }
  )

  mergedData
}

# Retrieves component information for the given Spark and Hadoop versions
spark_versions_info <- function(version, hadoop_version) {
  versions <- spark_versions()

  versions <- versions[versions$spark == version, ]
  if (NROW(versions) == 0) {
    stop("Spark version is not available")
  }

  versions <- versions[versions$hadoop == hadoop_version, ]
  if (NROW(versions) == 0) {
    stop("Hadoop version is not available")
  }

  version <- versions[1,]

  componentName <- paste0("spark-", version$spark, "-bin-hadoop", version$hadoop)
  packageName <- paste0(componentName, ".tgz")
  packageRemotePath <- version$download

  list (
    componentName = componentName,
    packageName = packageName,
    packageRemotePath = packageRemotePath
  )
}
