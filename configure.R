#!/usr/bin/env Rscript
library(sparklyr)
scalac <- path.expand("~/scala-2.10.6/bin/scalac")
sparklyr::compile_package_jars(
  spark_versions = c("1.6.2", "2.0.0"),
  scalac = scalac
)
