#!/usr/bin/env Rscript
library(sparklyr)
sparklyr:::livy_sources_refresh()

sparklyr:::spark_compile_embedded_sources()

javaopts <- Sys.getenv("CLASSPATH")
Sys.setenv(JAVA_OPTS = "-Xss256m -Xmx4096m")
on.exit(Sys.setenv(JAVA_OPTS = javaopts), add = TRUE)

sparklyr::compile_package_jars()
