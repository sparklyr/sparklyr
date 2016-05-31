#' Retrieves available versions of Spark
#' @name spark_versions
#' @export
spark_versions <- function() {
  latestUrl <- "https://raw.githubusercontent.com/rstudio/rspark/master/inst/extdata/install_spark.csv"
  packagePath <- system.file(file.path("extdata", "install_spark.csv"), package = "rspark")

  downloadData <- NULL
  tryCatch({
    suppressWarnings(
      downloadData <- read.csv(latestUrl, stringsAsFactors = FALSE)
    )
  }, error = function(e) {
  })

  if (is.null(downloadData) || is.null(downloadData$spark)) {
    warning("Failed to retrieve the latest download links")
    downloadData <- read.csv(packagePath, stringsAsFactors = FALSE)
  }

  downloadData$installed <- rep(FALSE, NROW(downloadData))

  mergedData <- downloadData
  lapply(
    Filter(function(e) !is.null(e),
      lapply(dir(spark_install_dir(), full.names = TRUE), function(maybeDir) {
        if (dir.exists(maybeDir)) {
          installDir <- maybeDir
          m <- regmatches(installDir, regexec(".*spark-(.*)-bin-hadoop(.*)", installDir))[[1]]
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

      mergedData <<- rbind(notCurrentRow, newRow)
    }
  )

  mergedData
}

#' Retrieves component information for the given Spark and Hadoop versions
#' @name spark_versions_info
#' @export
#' @param version The Spark version.
#' @param hadoop_version The Hadoop version.
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
