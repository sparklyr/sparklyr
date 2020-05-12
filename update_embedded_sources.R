#!/usr/bin/env Rscript

sparklyr:::spark_update_embedded_sources(
  jars_to_skip = "sparklyr-1.5-2.10.jar"
)
