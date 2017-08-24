#!/usr/bin/env Rscript
library(sparklyr)
sparklyr:::ml_create_param_mapping_tables()
sparklyr:::livy_sources_refresh()

sparklyr:::spark_compile_embedded_sources()

javaopts <- Sys.getenv("JAVA_OPTS")
Sys.setenv(JAVA_OPTS = "-Xss256m -Xmx4096m")
on.exit(Sys.setenv(JAVA_OPTS = javaopts), add = TRUE)

sparklyr::compile_package_jars()
