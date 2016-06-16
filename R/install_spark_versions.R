spark_versions_file_pattern <- function() {
  "spark-(.*)-bin-hadoop(.*)"
}

#' Retrieves available versions of Spark
#' @rdname spark_install
#' @export
spark_versions <- function(latest = FALSE) {
  latestUrl <- "https://raw.githubusercontent.com/rstudio/sparklyr/master/inst/extdata/install_spark.csv?token=ASpg1NOA-Y-_Ir67ZLqzefBWo8URFxO5ks5XYZCAwA%3D%3D"
  packagePath <- system.file(file.path("extdata", "install_spark.csv"), package = "sparklyr")

  downloadData <- NULL
  if (latest) {
    tryCatch({
      suppressWarnings(
        downloadData <- utils::read.csv(latestUrl,
                                        colClasses = c(hadoop = "character"),
                                        stringsAsFactors = FALSE)
      )
    }, error = function(e) {
    })
  }

  if (is.null(downloadData) || is.null(downloadData$spark)) {
    # warning("Failed to retrieve the latest download links")
    downloadData <- utils::read.csv(packagePath,
                                    colClasses = c(hadoop = "character"),
                                    stringsAsFactors = FALSE)
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
