#' Cross Tabulation
#'
#' Builds a contingency table at each combination of factor levels.
#'
#' @param x A Spark DataFrame
#' @param col1 The name of the first column. Distinct items will make the first item of each row.
#' @param col2 The name of the second column. Distinct items will make the column names of the DataFrame.
#' @return A DataFrame containing the contingency table.
#' @export
sdf_crosstab <- function(x, col1, col2) {
  col1 <- cast_string(col1)
  col2 <- cast_string(col2)

  x %>%
    spark_dataframe() %>%
    invoke("%>%", list("stat"), list("crosstab", col1, col2)) %>%
    sdf_register()
}

#' Generate random samples from some distribution
#'
#' Generator methods for creating single-column Spark dataframes comprised of
#' i.i.d. samples from some distribution.
#'
#' @param sc A Spark connection.
#' @param n Sample Size (default: 1000).
#' @param num_partitions Number of partitions in the resulting Spark dataframe
#'   (default: default parallelism of the Spark cluster).
#' @param seed Random seed (default: a random long integer).
#' @param output_col Name of the output column containing sample values (default: "x").
#'
#' @name spark_statistical_routines
NULL

gen_sample_sdf <- function(
                           sc,
                           method,
                           dist_params,
                           n,
                           num_partitions,
                           seed,
                           output_col,
                           cls = "org.apache.spark.mllib.random.RandomRDDs") {
  num_partitions <- as.integer(num_partitions %||%
    tryCatch(spark_context(sc) %>% invoke("defaultParallelism"), error = function(e) 4L)
  )
  seed <- as.numeric(seed %||% Sys.time())
  columns <- list("double")
  names(columns) <- output_col
  schema <- spark_data_build_types(sc, columns)
  do.call(
    invoke_static,
    list(sc, cls, method, spark_context(sc)) %>%
    append(unname(dist_params)) %>%
    append(list(n, num_partitions, seed))
  ) %>%
    invoke_static(sc, "sparklyr.RddUtils", "toRowRDD", .) %>%
    invoke(spark_session(sc), "createDataFrame", ., schema) %>%
    sdf_register()
}

#' Generate random samples from an exponential distribution
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from an exponential distribution.
#'
#' @inheritParams spark_statistical_routines
#' @param rate Rate of the exponential distribution (default: 1). The exponential
#'   distribution with rate lambda has mean 1 / lambda and density f(x) = lambda {e}^{- lambda x}.
#'
#' @family Spark statistical routines
#' @export
sdf_rexp <- function(sc, n, rate = 1, num_partitions = NULL, seed = NULL, output_col = "x") {
  gen_sample_sdf(
    sc,
    method = "exponentialRDD",
    dist_params = list(mean = 1 / rate),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col
  )
}

#' Generate random samples from a Gamma distribution
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from a Gamma distribution.
#'
#' @inheritParams spark_statistical_routines
#' @param shape Shape parameter (greater than 0) for the Gamma distribution.
#' @param rate Rate parameter (greater than 0) for the Gamma distribution (scale is 1/rate).
#'
#' @family Spark statistical routines
#' @export
sdf_rgamma <- function(sc, n, shape, rate = 1, num_partitions = NULL, seed = NULL, output_col = "x") {
  gen_sample_sdf(
    sc,
    method = "gammaRDD",
    dist_params = list(shape = shape, scale = 1 / rate),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col
  )
}

#' Generate random samples from a log normal distribution
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from a log normal distribution.
#'
#' @inheritParams spark_statistical_routines
#' @param meanlog The mean of the normally distributed natural logarithm of this distribution.
#' @param sdlog The Standard deviation of the normally distributed natural logarithm of this distribution.
#'
#' @family Spark statistical routines
#' @export
sdf_rlnorm <- function(sc, n, meanlog = 0, sdlog = 1, num_partitions = NULL, seed = NULL, output_col = "x") {
  gen_sample_sdf(
    sc,
    method = "logNormalRDD",
    dist_params = list(mean = meanlog, std = sdlog),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col
  )
}

#' Generate random samples from the standard normal distribution
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from the standard normal distribution.
#'
#' @inheritParams spark_statistical_routines
#'
#' @family Spark statistical routines
#' @export
sdf_rnorm <- function(sc, n, mean = 0, sd = 1, num_partitions = NULL, seed = NULL, output_col = "x") {
  standard_normal_dist <- (mean == 0 && sd == 1)
  cls <- (
    if (standard_normal_dist) {
      "org.apache.spark.mllib.random.RandomRDDs"
    } else {
      "sparklyr.RandomRDDs"
    }
  )

  gen_sample_sdf(
    sc,
    method = "normalRDD",
    dist_params = if (standard_normal_dist) list() else list(mean, sd),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col,
    cls = cls
  )
}

#' Generate random samples from a Poisson distribution
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from a Poisson distribution.
#'
#' @inheritParams spark_statistical_routines
#' @param lambda Mean, or lambda, of the Poisson distribution.
#'
#' @family Spark statistical routines
#' @export
sdf_rpois <- function(sc, n, lambda, num_partitions = NULL, seed = NULL, output_col = "x") {
  gen_sample_sdf(
    sc,
    method = "poissonRDD",
    dist_params = list(mean = lambda),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col
  )
}

#' Generate random samples from the uniform distribution U(0, 1).
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from the uniform distribution U(0, 1).
#'
#' @inheritParams spark_statistical_routines
#'
#' @family Spark statistical routines
#' @export
sdf_runif <- function(sc, n, min = 0, max = 1, num_partitions = NULL, seed = NULL, output_col = "x") {
  standard_unif_dist <- (min == 0 && max == 1)
  cls <- (
    if (standard_unif_dist) {
      "org.apache.spark.mllib.random.RandomRDDs"
    } else {
      "sparklyr.RandomRDDs"
    }
  )

  gen_sample_sdf(
    sc,
    method = "uniformRDD",
    dist_params = if (standard_unif_dist) list() else list(min, max),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col,
    cls = cls
  )
}
