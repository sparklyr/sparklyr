#!/usr/bin/env Rscript
options(repos = c(CRAN = "https://cran.rstudio.com"))

# Get potential installation paths
Sys.setenv(R_SPARKLYR_INSTALL_INFO_PATH = system.file(
  "extdata/install_spark.csv",
  package = "sparklyr")
)

spark_home_from_version <- function(spark_version, hadoop_version = "2.6") {
  install_info <- tryCatch(
    spark_install_find(spark_version, hadoop_version),
    error = function(e) {
      spark_install(spark_version, hadoop_version)
      spark_install_find(spark_version, hadoop_version)
    }
  )

  install_info$sparkVersionDir
}

sparkapi::spark_compile("sparklyr", spark_home = spark_home_from_version("1.6.1"))
sparkapi::spark_compile("sparklyr", spark_home = spark_home_from_version("2.0.0-preview"))
