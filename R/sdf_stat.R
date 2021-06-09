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

gen_samples_sdf <- function(
                            sc,
                            method,
                            dist_params,
                            n,
                            num_partitions,
                            seed,
                            output_col,
                            output_col_type = "double",
                            cls = "org.apache.spark.mllib.random.RandomRDDs") {
  num_partitions <- as.integer(num_partitions %||%
    tryCatch(spark_context(sc) %>% invoke("defaultParallelism"), error = function(e) 4L))
  seed <- as.numeric(seed %||% Sys.time())
  columns <- list(output_col_type)
  names(columns) <- output_col
  schema <- spark_data_build_types(sc, columns)
  do.call(
    invoke_static,
    list(sc, cls, method, spark_context(sc)) %>%
      append(unname(dist_params)) %>%
      append(list(n, num_partitions, seed))
  ) %>%
    invoke_static(sc, "sparklyr.RddUtils", paste0(output_col_type, "ToRow"), .) %>%
    invoke(spark_session(sc), "createDataFrame", ., schema) %>%
    sdf_register()
}

#' Generate random samples from a Beta distribution
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from a Betal distribution.
#'
#' @inheritParams spark_statistical_routines
#' @param shape1 Non-negative parameter (alpha) of the Beta distribution.
#' @param shape2 Non-negative parameter (beta) of the Beta distribution.
#'
#' @family Spark statistical routines
#' @export
sdf_rbeta <- function(sc, n, shape1, shape2, num_partitions = NULL, seed = NULL, output_col = "x") {
  gen_samples_sdf(
    sc,
    method = "betaRDD",
    dist_params = list(alpha = shape1, beta = shape2),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col,
    cls = "sparklyr.RandomRDDs"
  )
}

#' Generate random samples from a binomial distribution
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from a binomial distribution.
#'
#' @inheritParams spark_statistical_routines
#' @param size Number of trials (zero or more).
#' @param prob Probability of success on each trial.
#'
#' @family Spark statistical routines
#' @export
sdf_rbinom <- function(sc, n, size, prob, num_partitions = NULL, seed = NULL, output_col = "x") {
  gen_samples_sdf(
    sc,
    method = "binomialRDD",
    dist_params = list(trials = as.integer(size), p = prob),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col,
    output_col_type = "integer",
    cls = "sparklyr.RandomRDDs"
  )
}

#' Generate random samples from a Cauchy distribution
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from a Cauchy distribution.
#'
#' @inheritParams spark_statistical_routines
#' @param location Location parameter of the distribution.
#' @param scale Scale parameter of the distribution.
#'
#' @family Spark statistical routines
#' @export
sdf_rcauchy <- function(sc, n, location = 0, scale = 1, num_partitions = NULL, seed = NULL, output_col = "x") {
  gen_samples_sdf(
    sc,
    method = "cauchyRDD",
    dist_params = list(median = location, scale = scale),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col,
    cls = "sparklyr.RandomRDDs"
  )
}

#' Generate random samples from a chi-squared distribution
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from a chi-squared distribution.
#'
#' @inheritParams spark_statistical_routines
#' @param df Degrees of freedom (non-negative, but can be non-integer).
#'
#' @family Spark statistical routines
#' @export
sdf_rchisq <- function(sc, n, df, num_partitions = NULL, seed = NULL, output_col = "x") {
  gen_samples_sdf(
    sc,
    method = "chiSquaredRDD",
    dist_params = list(degreesOfFreedom = df),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col,
    cls = "sparklyr.RandomRDDs"
  )
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
  gen_samples_sdf(
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
  gen_samples_sdf(
    sc,
    method = "gammaRDD",
    dist_params = list(shape = shape, scale = 1 / rate),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col
  )
}

#' Generate random samples from a geometric distribution
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from a geometric distribution.
#'
#' @inheritParams spark_statistical_routines
#' @param prob Probability of success in each trial.
#'
#' @family Spark statistical routines
#' @export
sdf_rgeom <- function(sc, n, prob, num_partitions = NULL, seed = NULL, output_col = "x") {
  gen_samples_sdf(
    sc,
    method = "geometricRDD",
    dist_params = list(p = prob),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col,
    output_col_type = "integer",
    cls = "sparklyr.RandomRDDs"
  )
}

#' Generate random samples from a hypergeometric distribution
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from a hypergeometric distribution.
#'
#' @inheritParams spark_statistical_routines
#' @param nn Sample Size.
#' @param m The number of successes among the population.
#' @param n The number of failures among the population.
#' @param k The number of draws.
#'
#' @family Spark statistical routines
#' @export
sdf_rhyper <- function(sc, nn, m, n, k, num_partitions = NULL, seed = NULL, output_col = "x") {
  gen_samples_sdf(
    sc,
    method = "hypergeometricRDD",
    dist_params = list(
      populationSize = as.integer(m + n),
      numSuccesses = as.integer(m),
      numDraws = as.integer(k)
    ),
    n = nn,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col,
    output_col_type = "integer",
    cls = "sparklyr.RandomRDDs"
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
  gen_samples_sdf(
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
#' @param mean The mean value of the normal distribution.
#' @param sd The standard deviation of the normal distribution.
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
    })

  gen_samples_sdf(
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
  gen_samples_sdf(
    sc,
    method = "poissonRDD",
    dist_params = list(mean = lambda),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col
  )
}

#' Generate random samples from a t-distribution
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from a t-distribution.
#'
#' @inheritParams spark_statistical_routines
#' @param df Degrees of freedom (> 0, maybe non-integer).
#'
#' @family Spark statistical routines
#' @export
sdf_rt <- function(sc, n, df, num_partitions = NULL, seed = NULL, output_col = "x") {
  gen_samples_sdf(
    sc,
    method = "tRDD",
    dist_params = list(degreesOfFreedom = df),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col,
    cls = "sparklyr.RandomRDDs"
  )
}

#' Generate random samples from a Weibull distribution.
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from a Weibull distribution.
#'
#' @inheritParams spark_statistical_routines
#' @param shape The shape of the Weibull distribution.
#' @param scale The scale of the Weibull distribution (default: 1).
#'
#' @family Spark statistical routines
#' @export
sdf_rweibull <- function(sc, n, shape, scale = 1, num_partitions = NULL, seed = NULL, output_col = "x") {
  gen_samples_sdf(
    sc,
    method = "weibullRDD",
    dist_params = list(alpha = shape, beta = scale),
    n = n,
    num_partitions = num_partitions,
    seed = seed,
    output_col = output_col,
    cls = "sparklyr.RandomRDDs"
  )
}

#' Generate random samples from the uniform distribution U(0, 1).
#'
#' Generator method for creating a single-column Spark dataframes comprised of
#' i.i.d. samples from the uniform distribution U(0, 1).
#'
#' @inheritParams spark_statistical_routines
#' @param min The lower limit of the distribution.
#' @param max The upper limit of the distribution.
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
    })

  gen_samples_sdf(
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
