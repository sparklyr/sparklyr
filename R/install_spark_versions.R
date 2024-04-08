spark_versions_file_pattern <- function() {
  "spark-(.*)-bin-(?:hadoop)?(.*)"
}

spark_versions_url <- function() {
  "https://raw.githubusercontent.com/sparklyr/sparklyr/main/inst/extdata/versions.json"
}

#' @importFrom jsonlite fromJSON
read_spark_versions_json <- function(latest = TRUE, future = FALSE) {
  # see if we have a cached version
  if (is.null(genv_get_spark_versions_json())) {
    # This function might be called during a custom configuration and the package
    # will not be available at that time; allow overriding with environment variable
    package_path_env <- Sys.getenv("R_SPARKINSTALL_INSTALL_INFO_PATH", unset = NA)
    package_path <- if (!is.na(package_path_env)) {
      package_path_env
    } else {
      system.file(file.path("extdata", "versions.json"), package = packageName())
    }

    versions_json <- NULL

    if (is.na(package_path_env)) {
      if (latest) {
        versions_json <- tryCatch(
          {
            suppressWarnings(
              fromJSON(spark_versions_url(), simplifyDataFrame = TRUE)
            )
          },
          error = function(e) {
          }
        )
      }
    }

    if (is.null(versions_json)) {
      versions_json <- fromJSON(package_path, simplifyDataFrame = TRUE)
    }

    genv_set_spark_versions_json(versions_json)
  }

  if (identical(future, TRUE)) {
    # add future versions
    future_versions_path <- system.file(file.path("extdata", "versions-next.json"),
                                        package = packageName()
                                        )
    future_versions_json <- fromJSON(future_versions_path, simplifyDataFrame = TRUE)
    rbind(genv_get_spark_versions_json(), future_versions_json)
  }
  else {
    genv_get_spark_versions_json()
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

  rownames(versions) <- seq_len(nrow(versions))

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
  json_data <- read_spark_versions_json(latest, future = TRUE)

  json_data$installed <- FALSE

  json_data$download  <- json_data %>%
    transpose() %>%
    map_chr(~{
      paste0(.x$base, sprintf(.x$pattern, .x$spark, .x$hadoop))
      })

  json_data$default <- FALSE
  json_data$hadoop_default <- FALSE

  # apply spark and hadoop versions
  json_data[json_data$spark == "2.4.3" & json_data$hadoop == "2.7", ]$default <- TRUE

  json_data$hadoop_default <- json_data$spark %>%
    unique() %>%
    map(~ {
      spark <- json_data$spark == .x
      current <- json_data[spark, ]
      temp_hadoop <- ifelse(current$hadoop == "cdh4", 0, current$hadoop)
      temp_hadoop <- as.double(temp_hadoop)
      max_hadoop <- temp_hadoop == max(temp_hadoop)
      if(sum(max_hadoop) > 1) stop("Duplicate Spark + Hadoop combinations")
      max_hadoop
    }) %>%
    reduce(c)

  mergedData <- json_data
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
      currentRow <- json_data[json_data$spark == row$spark & json_data$hadoop == row$hadoop, ]
      notCurrentRow <- mergedData[mergedData$spark != row$spark | mergedData$hadoop != row$hadoop, ]

      newRow <- c(row, installed = TRUE)
      newRow$base <- ""
      newRow$download <- ""
      newRow$default <- FALSE
      newRow$hadoop_default <- FALSE

      if (NROW(currentRow) > 0) {
        currentRow$spark <- gsub("-preview", "", currentRow$spark)
        #hadoop_default <- if (compareVersion(currentRow$spark, "2.0") >= 0) "2.7" else "2.6"

        newRow$base <- currentRow$base
        newRow$pattern <- currentRow$pattern
        newRow$download <- currentRow$download
        newRow$default <- identical(currentRow$spark, "3.3.0")

        #newRow$hadoop_default <- identical(currentRow$hadoop, hadoop_default)
        newRow$hadoop_default <- currentRow$hadoop_default
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
