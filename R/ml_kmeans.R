spark_ml_kmeans <- function(x, centers, iter.max = 10) {
  scon <- spark_scon(x)
  df <- as_spark_dataframe(x)

  # collect vectors of interest into single column
  columns <- as.list(spark_invoke(df, "columns"))
  tdf <- spark_assemble_vector(scon, df, columns, "features")

  # invoke KMeans
  kmeans <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.clustering.KMeans"
  )

  kmm <- kmeans %>%
    spark_invoke("setK", as.integer(centers)) %>%
    spark_invoke("setMaxIter", as.integer(iter.max)) %>%
    spark_invoke("setFeaturesCol", "features") %>%
    spark_invoke("fit", tdf)

  kmm
}

#' Computes kmeans from a dplyr source
#' @export
#' @param x A dplyr source.
#' @param centers Number of centers to compute.
#' @param iter.max Maximum number of iterations used to compute kmeans.
ml_kmeans <- function(x, centers, iter.max = 10) {
  kmm <- spark_ml_kmeans(x, centers, iter.max)

  # extract cluster centers
  kmm_centers <- spark_invoke(kmm, "clusterCenters")

  centers_list <- transpose_list(lapply(kmm_centers, function(center) {
    unlist(spark_invoke(center, "toArray"), recursive = FALSE)
  }))

  names(centers_list) <- as.character(dplyr::tbl_vars(x))
  centers <- as.data.frame(centers_list, stringsAsFactors = FALSE)

  list(model = kmm, centers = centers)

}
