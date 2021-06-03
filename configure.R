#!/usr/bin/env Rscript
args <- commandArgs(trailingOnly = TRUE)

if (length(args) == 0) {
  targets <- c(
    "1.5.2",
    "1.6.0",
    "2.0.0",
    "2.3.0",
    "2.4.0",
    "3.0.0"
  )
} else if (length(args) == 1) {
  # default output file
  targets <- c(args[1])
} else {
  stop("Cannot take more than one argument.", call. = FALSE)
}

print(targets)

library(sparklyr)

sparklyr:::spark_gen_embedded_sources()

spec <- Filter(
  function(e) e$spark_version %in% targets,
  sparklyr::spark_default_compilation_spec()
)

sparklyr::compile_package_jars(spec = spec)

# for now, spark master and spark 3.0.0 are equivalent
java_dir <- file.path("inst", "java")
file.rename(file.path(java_dir, "sparklyr-3.0-2.12.jar"), file.path(java_dir, "sparklyr-master-2.12.jar"))
