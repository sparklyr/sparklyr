#!/usr/bin/env Rscript
options(repos = c(CRAN = "https://cran.rstudio.com"))

# Get potential installation paths
Sys.setenv(R_SPARKLYR_INSTALL_INFO_PATH = system.file(
  "extdata/install_spark.csv",
  package = "sparklyr")
)

spark_home_from_version <- function(spark_version, hadoop_version = "2.6") {
  spark_home <- spark_home_dir(spark_version, hadoop_version)
  if (is.null(spark_home)) {
    spark_install(spark_version, hadoop_version)
    spark_home_dir(spark_version, hadoop_version)
  }
  else
    spark_home
}

sparkapi::spark_compile("sparklyr", spark_home = spark_home_from_version("1.6.1"))
sparkapi::spark_compile("sparklyr", spark_home = spark_home_from_version("2.0.0-preview"))
