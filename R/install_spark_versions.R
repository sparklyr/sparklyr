# For a compact version of the mappings provided in this file see: http://spark.apache.org/js/downloads.js

h <- list(
  "2.6" =     list(name = "Hadoop 2.6 and later",     tag = "hadoop2.6",      supported = TRUE),
  "2.7" =     list(name = "Hadoop 2.7 and later",     tag = "hadoop2.7",      supported = TRUE),
  "cdh4" =    list(name = "CDH 4",                    tag = "cdh4",           supported = TRUE),

  "no" =      list(name = "User-provided Hadoop",     tag = "without-hadoop", supported = FALSE),
  "sources" = list(name = "Source Code",              tag = "sources",        supported = FALSE),
  "1.0" =     list(name = "Pre-built for Hadoop 1.X", tag = "hadoop1",        supported = FALSE),
  "2.0" =     list(name = "Hadoop 2.2",               tag = "hadoop2",        supported = FALSE),
  "2.3" =     list(name = "Hadoop 2.3",               tag = "hadoop2.3",      supported = FALSE),
  "2.4" =     list(name = "Hadoop 2.4 and later",     tag = "hadoop2.4",      supported = FALSE),
  "mapr3" =   list(name = "MapR 3.X",                 tag = "mapr3",          supported = FALSE),
  "mapr4" =   list(name = "MapR 4.X",                 tag = "mapr4",          supported = FALSE)
)

hv <- function(...) {
  args <- list(...)
  hvs <- h[unlist(args)]
  names(hvs) <- args
  hvs
}

pV1 <- hv("sources", "1.0", "cdh4")
pV2 <- c(hv("sources", "2.0"), pV1)
pV3 <- c(hv("sources", "mapr3", "mapr4"), pV2)
pV4 <- c(hv("sources", "2.4", "2.3", "mapr3", "mapr4"), pV1)
pV5 <- c(hv("sources", "2.6"), pV4)
pV6 <- c(hv("sources", "no", "2.6", "2.4", "2.3"), pV1)
pV7 <- hv("no", "2.3", "2.4", "2.6", "2.7")

releases <- list(
  "2.0.0" = list(hadoop = pV7, enabled = TRUE,  supported = TRUE),
  "1.6.1" = list(hadoop = pV6, enabled = TRUE,  supported = TRUE),
  "1.6.0" = list(hadoop = pV6, enabled = TRUE,  supported = TRUE),

  "1.5.2" = list(hadoop = pV6, enabled = TRUE,  supported = FALSE),
  "1.5.1" = list(hadoop = pV6, enabled = TRUE,  supported = FALSE),
  "1.5.0" = list(hadoop = pV6, enabled = TRUE,  supported = FALSE),
  "1.4.1" = list(hadoop = pV6, enabled = TRUE,  supported = FALSE),
  "1.4.0" = list(hadoop = pV6, enabled = TRUE,  supported = FALSE),
  "1.3.1" = list(hadoop = pV5, enabled = TRUE,  supported = FALSE),
  "1.3.0" = list(hadoop = pV4, enabled = TRUE,  supported = FALSE),
  "1.2.2" = list(hadoop = pV4, enabled = TRUE,  supported = FALSE),
  "1.2.1" = list(hadoop = pV4, enabled = TRUE,  supported = FALSE),
  "1.2.0" = list(hadoop = pV4, enabled = TRUE,  supported = FALSE),
  "1.1.1" = list(hadoop = pV4, enabled = TRUE,  supported = FALSE),
  "1.1.0" = list(hadoop = pV4, enabled = TRUE,  supported = FALSE),
  "1.0.2" = list(hadoop = pV3, enabled = TRUE,  supported = FALSE),
  "1.0.1" = list(hadoop = pV3, enabled = FALSE, supported = FALSE),
  "1.0.0" = list(hadoop = pV2, enabled = FALSE, supported = FALSE),
  "0.9.2" = list(hadoop = pV2, enabled = TRUE,  supported = FALSE),
  "0.9.1" = list(hadoop = pV2, enabled = FALSE, supported = FALSE),
  "0.9.0" = list(hadoop = pV2, enabled = FALSE, supported = FALSE),
  "0.8.1" = list(hadoop = pV2, enabled = TRUE,  supported = FALSE),
  "0.8.0" = list(hadoop = pV1, enabled = TRUE,  supported = FALSE),
  "0.7.3" = list(hadoop = pV1, enabled = TRUE,  supported = FALSE),
  "0.7.2" = list(hadoop = pV1, enabled = FALSE, supported = FALSE),
  "0.7.0" = list(hadoop = h$s, enabled = FALSE, supported = FALSE)
)

spark_versions_validate <- function(sparkVersion) {
  if (!sparkVersion %in% names(releases)) {
    stop("Spark version is not available")
  }
}

#' Retrieves available versions of Spark
#' @name spark_versions
#' @export
#' @param supported If TRUE (default), returns only the fully supported versions in this package.
#' Otherwise, retrieves all versions which may be only partially supported.
spark_versions <- function(supported = TRUE) {
  names(Filter(function(e) e$supported || !supported, releases))
}

#' Retrieves available versions of Hadoop for Spark
#' @name spark_versions_hadoop
#' @export
#' @param sparkVersion The Spark version to match for the available hadoop distributions
#' @param supported If TRUE (default), returns only the fully supported versions in this package.
#' Otherwise, retrieves all versions which may be only partially supported.
#' @return Named list of Hadoop versions supported by this version of spark.
spark_versions_hadoop <- function(sparkVersion = "1.6.0", supported = TRUE) {
  spark_versions_validate(sparkVersion)

  hadoopList <- releases[[sparkVersion]]$hadoop
  hadoopList <- Filter(function(e) e$supported || !supported, hadoopList)

  hadoopList
}

#' Opens the release notes for the given version of Spark
#' @name spark_versions_notes_url
#' @export
#' @param version The Spark version. See spark_versions for a list of available options.
spark_versions_notes_url <- function(version) {
  spark_versions_validate(version)

  if (version == "2.0.0") {
    stop("Not release notes available for this version")
  }

  link <- paste0("http://spark.apache.org/releases/spark-release-", gsub("\\.", "-", version), ".html")
  browseURL(link)
}

#' Retrieves download information for the given Spark and Hadoop versions
#' @name spark_versions_download_info
#' @export
#' @param sparkVersion The Spark version.
#' @param hadoopVersion The Hadoop version.
spark_versions_download_info <- function(sparkVersion, hadoopVersion) {
  parameterize <- function(source, sparkVersion, hadoopRelease) {
    source <- gsub("\\$ver", sparkVersion, source)
    source <- gsub("\\$pkg", hadoopRelease$tag, source)
    source <- gsub("-bin-sources", "", source)

    source
  }

  spark_versions_validate(sparkVersion)

  release <- releases[[sparkVersion]]

  if (!hadoopVersion %in% names(release$hadoop)) {
    stop("Hadoop version is not available")
  }
  hadoopRelease <- releases[[sparkVersion]]$hadoop[[hadoopVersion]]

  if (!release$supported) {
    warning(paste("Spark version", sparkVersion, "is only partially supported in rspark"))
  }

  if (!hadoopRelease$supported) {
    warning(paste("Hadoop version", sparkVersion, "is only partially supported in rspark"))
  }

  componentName <- parameterize("spark-$ver-bin-$pkg", sparkVersion, hadoopRelease)

  link = "http://d3kbcqa49mib13.cloudfront.net/";
  if (sparkVersion < "0.8.0") {
    link <- "http://spark-project.org/download/";
  }

  if (length(grep("mapr", hadoopVersion)) > 0) {
    link <- "http://package.mapr.com/tools/apache-spark/$ver/"
  }

  if (sparkVersion == "2.0.0") {
    link <- "http://people.apache.org/~pwendell/spark-nightly/spark-master-bin/latest/"
    componentName <- parameterize("spark-$ver-SNAPSHOT-bin-$pkg", sparkVersion, hadoopRelease)
  }

  packageName <- paste0(componentName, ".tgz")
  packageSource <- parameterize(link, sparkVersion, hadoopRelease)
  packageRemotePath <- parameterize(paste0(link, packageName), sparkVersion, hadoopRelease)

  list (
    componentName = componentName,
    packageName = packageName,
    packageSource = packageSource,
    packageRemotePath = packageRemotePath
  )
}

