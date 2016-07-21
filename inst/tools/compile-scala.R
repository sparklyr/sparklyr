#!/usr/bin/env Rscript
options(repos = c(CRAN = "https://cran.rstudio.com"))

spark_compile(spark_version = "1.6.1")
spark_compile(spark_version = "2.0.0-preview")
