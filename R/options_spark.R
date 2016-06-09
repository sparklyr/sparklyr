#' The rspark.dplyr.optimize_shuffle_cores option optimizes the number of partitions
#' to use when shuffling data based on the number of local cores.
#' This option is only applicable to local installations. The default TRUE provides
#' automatic detection of cores. To avoid using this setting, set this value to FALSE.
#' @name options-spark
#' @examples
#' getOption("rspark.dplyr.optimize_shuffle_cores", TRUE)
NULL

#' The rspark.packages.default option, overrides the default Spark packages
#' @name options-spark
#' @examples
#' getOption("rspark.packages.default", NULL)
NULL

#' The rspark.install.dir option, defines the default Spark installation directory
#' @name options-spark
#' @examples
#' getOption("rspark.install.dir", TRUE)
NULL

#' The spark.connection.allow_local_reconnect option, allows reconnect to
#' be enabled in local installations to help troubleshoot remote environments.
#' @name options-spark
#' @examples
#' getOption("spark.connection.allow_local_reconnect", FALSE)
NULL

#' The spark.connection.codegen option, sets the spark.sql.codegen.wholeStage option.
#' @name options-spark
#' @examples
#' getOption("spark.connection.codegen", FALSE)
NULL
