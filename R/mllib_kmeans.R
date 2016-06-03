#' Computes kmeans from a dplyr source
#' @export
#' @param x A dplyr source.
#' @param centers Number of centers to compute.
#' @param iter.max Maximum number of iterations used to compute kmeans.
spark_mllib_kmeans <- function(x, centers, iter.max = 10) {
  scon <- spark_scon(x)
  spark_rdd <- as_spark_rdd(x)

  kmeans_model <- spark_invoke_static(
    scon,

    "org.apache.spark.mllib.clustering.KMeans",
    "train",

    spark_rdd,
    as.integer(centers),
    as.integer(iter.max)
  )

  kmeans_model
}
