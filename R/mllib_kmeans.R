#' Computes kmeans from a dplyr source
#' @export
#' @param x A dplyr source.
#' @param centers Number of centers to compute.
#' @param iter.max Maximum number of iterations used to compute kmeans.
spark_mllib_kmeans <- function(x, centers, iter.max = 10) {
  scon <- spark_scon(x)
  sparkRdd <- as_spark_rdd(x)

  kmeansModel <- spark_invoke_static(
    scon,

    "org.apache.spark.mllib.clustering.KMeans",
    "train",

    sparkRdd,
    as.integer(centers),
    as.integer(iter.max)
  )

  list(
    model = kmeansModel,
    centers = spark_invoke(kmeansModel, "clusterCenters") %>%
      spark_jobj_list_to_array_df(colnames(x))
  )
}

