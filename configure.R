#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)

if (length(args)==0) {
  targets <- c("1.5.2",
               "1.6.0",
               "2.0.0",
               "2.3.0",
               "2.4.0")
} else if (length(args)==1) {
  # default output file
  targets <- c(args[1])
} else {
  stop("Cannot take more than one arguments.n", call.=FALSE)
}

print(targets)

library(sparklyr)
sparklyr:::livy_sources_refresh()

sparklyr:::spark_compile_embedded_sources()

spec <- Filter(
  function(e) e$spark_version %in% targets,
  sparklyr::spark_default_compilation_spec()
)

spec <- c(
  spec,
  spark_compilation_spec(
    spark_version = "master",
    scalac_path = find_scalac("2.12", locations),
    jar_name = sprintf("%s-master-2.12.jar", pkg),
    jar_path = find_jar(),
    scala_filter = make_version_filter("master")
  )
)

sparklyr::compile_package_jars(spec = spec)
