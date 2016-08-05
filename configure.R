#!/usr/bin/env Rscript
library(sparklyr)
scalac <- path.expand("~/scala-2.10.6/bin/scalac")
sparklyr::spark_compile_package_jars(scalac = scalac)
