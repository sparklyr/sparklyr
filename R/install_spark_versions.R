.globals <- new.env(parent = emptyenv())

spark_versions_file_pattern <- function() {
  "spark-(.*)-bin-(?:hadoop)?(.*)"
}

spark_versions_url <- function() {
  "https://raw.githubusercontent.com/sparklyr/sparklyr/master/inst/extdata/versions.json"
}

#' @importFrom jsonlite fromJSON
read_spark_versions_json <- function(latest = TRUE, future = FALSE) {
  # see if we have a cached version
  if (!exists("sparkVersionsJson", envir = .globals)) {
    # This function might be called during a custom configuration and the package
    # will not be available at that time; allow overriding with environment variable
    packagePathEnv <- Sys.getenv("R_SPARKINSTALL_INSTALL_INFO_PATH", unset = NA)
    packagePath <- if (!is.na(packagePathEnv)) {
      packagePathEnv
    } else {
      system.file(file.path("extdata", "versions.json"), package = packageName())
    }

    versionsJson <- NULL
    if (latest) {
      versionsJson <- tryCatch(
        {
          suppressWarnings(
            fromJSON(spark_versions_url(), simplifyDataFrame = TRUE)
          )
        },
        error = function(e) {
        }
      )
    }

    if (is.null(versionsJson)) {
      versionsJson <- fromJSON(packagePath, simplifyDataFrame = TRUE)
    }

    assign("sparkVersionsJson", versionsJson, envir = .globals)
  }

  if (identical(future, TRUE)) {
    # add future versions
    futureVersionsPath <- system.file(file.path("extdata", "versions-next.json"), package = packageName())
    futureVersionsJson <- fromJSON(futureVersionsPath, simplifyDataFrame = TRUE)
    versionsJson <- rbind(.globals$sparkVersionsJson, futureVersionsJson)
  }
  else {
    .globals$sparkVersionsJson
  }
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
  versions <- data.frame(
    spark = spark,
    hadoop = hadoop,
    dir = file.path(spark_install_dir(), dir),
    stringsAsFactors = FALSE
  )

  versions
}

#' @param show_hadoop Show Hadoop distributions?
#' @param show_minor Show minor Spark versions?
#' @param show_future Should future versions which have not been released be shown?
#'
#' @rdname spark_install
#'
#' @export
spark_available_versions <- function(show_hadoop = FALSE, show_minor = FALSE, show_future = FALSE) {
  versions <- read_spark_versions_json(latest = TRUE, future = show_future)
  versions <- versions[versions$spark >= "1.6.0", 1:2]
  selection <- if (show_hadoop) c("spark", "hadoop") else "spark"

  versions <- unique(subset(versions, select = selection))

  rownames(versions) <- 1:nrow(versions)

  if (!show_minor) versions$spark <- gsub("\\.[0-9]+$", "", versions$spark)

  versions <- unique(versions)
  rownames(versions) <- NULL

  versions
}

#' Retrieves a dataframe available Spark versions that van be installed.
#'
#' @param latest Check for latest version?
#'
#' @keywords internal
#' @export
spark_versions <- function(latest = TRUE) {
  downloadData <- read_spark_versions_json(latest, future = TRUE)
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
  downloadData[downloadData$spark == "2.4.3" & downloadData$hadoop == "2.7", ]$default <- TRUE
  lapply(unique(downloadData$spark), function(version) {
    validVersions <- downloadData[grepl("2", downloadData$hadoop) & downloadData$spark == version, ]
    maxHadoop <- validVersions[with(validVersions, order(hadoop, decreasing = TRUE)), ]$hadoop[[1]]

    downloadData[downloadData$spark == version & downloadData$hadoop == maxHadoop, ]$hadoop_default <<- TRUE
  })

  mergedData <- downloadData
  lapply(
    Filter(
      function(e) !is.null(e),
      lapply(dir(c(spark_install_old_dir(), spark_install_dir()), full.names = TRUE), function(maybeDir) {
        if (dir.exists(maybeDir)) {
          fileName <- basename(maybeDir)
          m <- regmatches(fileName, regexec(spark_versions_file_pattern(), fileName))[[1]]
          if (length(m) > 2) list(spark = m[[2]], hadoop = m[[3]], pattern = fileName) else NULL
        }
      })
    ),
    function(row) {
      currentRow <- downloadData[downloadData$spark == row$spark & downloadData$hadoop == row$hadoop, ]
      notCurrentRow <- mergedData[mergedData$spark != row$spark | mergedData$hadoop != row$hadoop, ]

      newRow <- c(row, installed = TRUE)
      newRow$base <- ""
      newRow$download <- ""
      newRow$default <- FALSE
      newRow$hadoop_default <- FALSE

      if (NROW(currentRow) > 0) {
        currentRow$spark <- gsub("-preview", "", currentRow$spark)
        hadoop_default <- if (compareVersion(currentRow$spark, "2.0") >= 0) "2.7" else "2.6"

        newRow$base <- currentRow$base
        newRow$pattern <- currentRow$pattern
        newRow$download <- currentRow$download
        newRow$default <- identical(currentRow$spark, "3.0.0")
        newRow$hadoop_default <- identical(currentRow$hadoop, hadoop_default)
      }

      mergedData <<- rbind(notCurrentRow, newRow)
    }
  )

  mergedData
}

# Retrieves component information for the given Spark and Hadoop versions
spark_versions_info <- function(version, hadoop_version, latest = TRUE) {
  versions <- spark_versions(latest = latest)

  versions <- versions[versions$spark == version, ]
  if (NROW(versions) == 0) {
    stop("Spark version is not available")
  }

  versions <- versions[versions$hadoop == hadoop_version, ]
  if (NROW(versions) == 0) {
    stop("Hadoop version is not available")
  }

  version <- versions[1, ]

  if (nchar(versions$pattern) > 0) {
    componentName <- sub("\\.tgz", "", sprintf(versions$pattern, version$spark, version$hadoop))
  }
  else {
    componentName <- sprintf("spark-%s-bin-hadoop%s", version$spark, version$hadoop)
  }

  packageName <- paste0(componentName, ".tgz")
  packageRemotePath <- version$download

  list(
    componentName = componentName,
    packageName = packageName,
    packageRemotePath = packageRemotePath
  )
}
