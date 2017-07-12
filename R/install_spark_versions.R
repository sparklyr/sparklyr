.globals <- new.env(parent = emptyenv())

spark_versions_file_pattern <- function() {
  "spark-(.*)-bin-(?:hadoop)?(.*)"
}

spark_versions_url <- function() {
  "https://raw.githubusercontent.com/rstudio/spark-install/master/common/versions.json"
}

#' @importFrom jsonlite fromJSON
read_spark_versions_json <- function(file = spark_versions_url()) {

  # see if we have a cached version
  if (!exists("sparkVersionsJson", envir = .globals))
  {
    versionsJson <- fromJSON(file, simplifyDataFrame = TRUE)
    assign("sparkVersionsJson", versionsJson, envir = .globals)
  }

  .globals$sparkVersionsJson
}


#' @rdname spark_install
#'
#' @export
spark_installed_versions <- function() {

  spark <- character()
  hadoop <- character()
  dir <- character()
  lapply(dir(c(spark_install_old_dir(), spark_install_dir()), full.names = TRUE), function(maybeDir) {
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
  versions <- read_spark_versions_json()
  versions <- versions[versions$spark >= "1.6.0", 1:2]
  versions$install <- paste0("spark_install(version = \"",
                             versions$spark, "\", ",
                             "hadoop_version = \"", versions$hadoop,
                             "\")")
  versions
}

#' Retrieves a dataframe available Spark versions that van be installed.
#'
#' @param latest Check for latest version?
#'
#' @keywords internal
#' @export
spark_versions <- function(latest = TRUE) {

  # This function might be called during a custom configuration and the package
  # will not be available at that time; allow overriding with environment variable
  packagePathEnv <- Sys.getenv("R_SPARKINSTALL_INSTALL_INFO_PATH", unset = NA)
  packagePath <- if (!is.na(packagePathEnv))
    packagePathEnv
  else
    system.file(file.path("extdata", "versions.json"), package = packageName())

  downloadData <- NULL
  if (latest) {
    tryCatch({
      suppressWarnings(
        downloadData <- read_spark_versions_json()
      )
    }, error = function(e) {
    })
  }

  if (is.null(downloadData) || is.null(downloadData$spark)) {
    downloadData <- read_spark_versions_json(packagePath)
  }

  downloadData$installed <- rep(FALSE, NROW(downloadData))

  downloadData$download <- paste(
    downloadData$base,
    mapply(function(pattern, spark, hadoop) {
      sprintf(pattern, spark, hadoop)
    }, downloadData$pattern, downloadData$spark, downloadData$hadoop),
    sep = ""
  )

  downloadData$default <- rep(FALSE, NROW(downloadData))
  downloadData$hadoop_default <- rep(FALSE, NROW(downloadData))

  # apply spark and hadoop versions
  downloadData[downloadData$spark == "2.1.0" & downloadData$hadoop == "2.7", ]$default <- TRUE
  lapply(unique(downloadData$spark), function(version) {
    validVersions <- downloadData[grepl("2", downloadData$hadoop) & downloadData$spark == version, ]
    maxHadoop <- validVersions[with(validVersions, order(hadoop, decreasing = TRUE)), ]$hadoop[[1]]

    downloadData[downloadData$spark == version & downloadData$hadoop == maxHadoop, ]$hadoop_default <<- TRUE
  })

  mergedData <- downloadData
  lapply(
    Filter(function(e) !is.null(e),
           lapply(dir(c(spark_install_old_dir(), spark_install_dir()), full.names = TRUE), function(maybeDir) {
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
      newRow$base <- if (NROW(currentRow) > 0) currentRow$base else ""
      newRow$pattern <- if (NROW(currentRow) > 0) currentRow$pattern else ""
      newRow$download <- if (NROW(currentRow) > 0) currentRow$download else ""
      newRow$default <- identical(currentRow$spark, "2.1.0")
      newRow$hadoop_default <- if (compareVersion(currentRow$spark, "2.0") >= 0)
          identical(currentRow$hadoop, "2.7")
        else
          identical(currentRow$hadoop, "2.6")

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

  componentName <- sub("\\.tgz", "", sprintf(versions$pattern, version$spark, version$hadoop))
  packageName <- paste0(componentName, ".tgz")
  packageRemotePath <- version$download

  list (
    componentName = componentName,
    packageName = packageName,
    packageRemotePath = packageRemotePath
  )
}
