#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)

if (length(args)==0) {
  targets <- c("1.5.2",
               "1.6.0",
               "2.0.0",
               "2.3.0",
               "2.4.0",
               "3.0.0-preview")
} else if (length(args)==1) {
  # default output file
  targets <- c(args[1])
} else {
  stop("Cannot take more than one argument.", call.=FALSE)
}

print(targets)

library(sparklyr)

sparklyr:::spark_compile_embedded_sources()

spec <- Filter(
  function(e) e$spark_version %in% targets,
  sparklyr::spark_default_compilation_spec()
)

# compile spark preview
spec[[length(spec) + 1]] <- spark_compilation_spec(
    spark_version = "3.0.0-preview",
    scalac_path = find_scalac("2.12"),
    jar_name = "sparklyr-3.0.0-preview-2.12.jar",
    jar_path = NULL,
    scala_filter = sparklyr:::make_version_filter("3.0.0")
  )

sparklyr::compile_package_jars(spec = spec)

# for now, spark master and spark 3.0.0-preview are equivalent
file.copy("inst/java/sparklyr-3.0.0-preview-2.12.jar", "inst/java/sparklyr-master-2.12.jar", overwrite = TRUE)

