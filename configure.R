#!/usr/bin/env Rscript
library(sparklyr)
sparklyr:::livy_sources_refresh()
sparklyr::compile_package_jars()
