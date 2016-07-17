#!/usr/bin/env Rscript
options(repos = c(CRAN = "https://cran.rstudio.com"))

if (!requireNamespace("rprojroot", quietly = TRUE))
  install.packages("rprojroot")
library(rprojroot)
root <- rprojroot::find_package_root_file()

Sys.setenv(R_SPARKLYR_INSTALL_INFO_PATH = file.path(root, "inst/extdata/install_spark.csv"))

if (!requireNamespace("digest", quietly = TRUE))
  install.packages("digest")
library(digest)

sparklyr_utils_path <- file.path(root, "inst/java/sparklyr_utils.jar")
sparklyr_scala <- file.path(root, "inst/scala/sparklyr.scala")
sparklyr_scala_digest <- file.path(root, "inst/scala/sparklyr.scala.md5")

# Bail if 'sparklyr.scala' hasn't changed
md5 <- tools::md5sum(sparklyr_scala)
if (file.exists(sparklyr_scala_digest) && file.exists(sparklyr_utils_path)) {
  contents <- readChar(sparklyr_scala_digest, file.info(sparklyr_scala_digest)$size, TRUE)
  if (identical(contents, md5[[sparklyr_scala]])) {
    stop()
  }
}

message("** building 'sparklyr_utils.jar' ...")

cat(md5, file = sparklyr_scala_digest)

execute <- function(...) {
  cmd <- paste(...)
  message("*** ", cmd)
  system(cmd)
}

if (!nzchar(Sys.which("scalac")))
  stop("failed to discover 'scalac' on the PATH")

if (!nzchar(Sys.which("jar")))
  stop("failed to discover 'jar' on the PATH")

# Work in temporary directory (as temporary class files
# will be generated within there)
dir <- file.path(tempdir(), "sparklyr-scala-compile")
if (!file.exists(dir))
  if (!dir.create(dir))
    stop("Failed to create '", dir, "'")
owd <- setwd(dir)

spark_version <- "1.6.1"
hadoop_version <- "2.6"

# Get potential installation paths
install_info <- tryCatch(
  spark_install_find(spark_version, hadoop_version),
  error = function(e) {
    spark_install(spark_version, hadoop_version)
    spark_install_find(spark_version, hadoop_version)
  }
)

# list jars in the installation folder
candidates <- c("jars", "lib")
jars <- NULL
for (candidate in candidates) {
  jars <- list.files(
    file.path(install_info$sparkVersionDir, candidate),
    full.names = TRUE,
    pattern = "jar$"
  )

  if (length(jars))
    break
}

if (!length(jars))
  stop("failed to discover Spark jars")

# construct classpath
CLASSPATH <- paste(jars, collapse = .Platform$path.sep)

# ensure 'inst/java' exists
inst_java_path <- file.path(root, "inst/java")
if (!file.exists(inst_java_path))
  if (!dir.create(inst_java_path, recursive = TRUE))
    stop("failed to create directory '", inst_java_path, "'")

# call 'scalac' compiler
classpath <- Sys.getenv("CLASSPATH")

# set CLASSPATH environment variable rather than passing
# in on command line (mostly aesthetic)
Sys.setenv(CLASSPATH = CLASSPATH)
execute("scalac", shQuote(sparklyr_scala))
Sys.setenv(CLASSPATH = classpath)

# call 'jar' to create our jar
class_files <- list.files(pattern = "class$")
execute("jar cf", sparklyr_utils_path, paste(shQuote(class_files), collapse = " "))

# double-check existence of 'sparklyr_utils.jar'
if (file.exists(sparklyr_utils_path)) {
  message("*** ", basename(sparklyr_utils_path), " successfully created.")
} else {
  stop("*** failed to create sparklyr_utils.jar")
}

setwd(owd)
