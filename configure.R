#!/usr/bin/env Rscript
library(sparklyr)
sparklyr:::livy_sources_refresh()

sparklyr:::spark_compile_embedded_sources()

sparklyr::compile_package_jars()
