#' The rspark.dplyr.optimizeShuffleForCores option optimizes the number of partitions
#' to use when shuffling data based on the number of local cores.
#' This option is only applicable to local installations. The default TRUE provides
#' automatic detection of cores. To avoid using this setting, set this value to FALSE.
#' @name dplyr-spark-options-optimizeShuffleForCores
#' @examples
#' getOption("rspark.dplyr.optimizeShuffleForCores", TRUE)
NULL
