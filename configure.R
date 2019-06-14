#!/usr/bin/env Rscript
library(sparklyr)
sparklyr:::livy_sources_refresh()

sparklyr:::spark_compile_embedded_sources()

targets <- c("1.5.2",
             "1.6.0",
             "2.0.0",
             "2.3.0")

spec <- Filter(
  function(e) e$spark_version %in% targets,
  sparklyr::spark_default_compilation_spec()
)

sparklyr::compile_package_jars(spec = spec)
