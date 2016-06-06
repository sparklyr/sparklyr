#!/usr/bin/env Rscript

if (!requireNamespace("rprojroot", quietly = TRUE))
  install.packages("rprojroot")
library(rprojroot)
root <- rprojroot::find_package_root_file()

# Bail if 'rspark.scala' hasn't changed
if (!requireNamespace("digest", quietly = TRUE))
  install.packages("digest")
library(digest)

rspark_utils_path <- file.path(root, "inst/java/rspark_utils.jar")
rspark_scala <- file.path(root, "inst/scala/rspark.scala")
rspark_scala_digest <- file.path(root, "inst/scala/rspark.scala.md5")

md5 <- tools::md5sum(rspark_scala)
if (file.exists(rspark_scala_digest) && file.exists(rspark_utils_path)) {
  contents <- readChar(rspark_scala_digest, file.info(rspark_scala_digest)$size, TRUE)
  if (identical(contents, md5[[rspark_scala]])) {
    stop("*** Skipping compilation of 'rspark.scala': contents unchanged")
  }
}

cat(md5, file = rspark_scala_digest)

execute <- function(...) {
  cmd <- paste(...)
  message("*** ", cmd)
  system(cmd)
}

if (!nzchar(Sys.which("scalac")))
  stop("Failed to discover 'scalac' on the PATH")

if (!nzchar(Sys.which("jar")))
  stop("Failed to discover 'jar' on the PATH")

# Work in temporary directory (as temporary class files
# will be generated within there)
dir <- file.path(tempdir(), "rspark-scala-compile")
if (!file.exists(dir))
  if (!dir.create(dir))
    stop("Failed to create '", dir, "'")
owd <- setwd(dir)

spark_version <- "2.0.0-preview"
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
jars <- list.files(
  file.path(install_info$sparkVersionDir, "jars"),
  full.names = TRUE,
  pattern = "jar$"
)

# construct classpath
CLASSPATH <- paste(jars, collapse = .Platform$path.sep)

# ensure 'inst/java' exists
inst_java_path <- file.path(root, "inst/java")
if (!file.exists(inst_java_path))
  if (!dir.create(inst_java_path, recursive = TRUE))
    stop("Failed to create directory '", inst_java_path, "'")

# call 'scalac' compiler
classpath <- Sys.getenv("CLASSPATH")

# set CLASSPATH environment variable rather than passing
# in on command line (mostly aesthetic)
Sys.setenv(CLASSPATH = CLASSPATH)
execute("scalac", shQuote(rspark_scala))
Sys.setenv(CLASSPATH = classpath)

# call 'jar' to create our jar
class_files <- list.files(pattern = "class$")
execute("jar cf", rspark_utils_path, paste(shQuote(class_files), collapse = " "))

# double-check existence of 'rspark_utils.jar'
if (file.exists(rspark_utils_path)) {
  message("*** ", basename(rspark_utils_path), " successfully created.")
} else {
  stop("*** Failed to create rspark utils .jar")
}

setwd(owd)
