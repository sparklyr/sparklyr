spark_ml_kmeans <- function(x, centers, iter.max = 10, features = dplyr::tbl_vars(x)) {
  scon <- spark_scon(x)
  df <- as_spark_dataframe(x)

  # collect vectors of interest into single column
  if (is.null(features))
    features <- as.list(spark_invoke(df, "columns"))

  tdf <- spark_assemble_vector(scon, df, features, "features")

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
#' @param features Which columns to use in the kmeans fit. Defaults to
#'   all columns within \code{x}.
ml_kmeans <- function(x, centers, iter.max = 10, features = dplyr::tbl_vars(x)) {
  model <- spark_ml_kmeans(x, centers, iter.max, features)

  # extract cluster centers
  kmm_centers <- spark_invoke(model, "clusterCenters")

  centers_list <- transpose_list(lapply(kmm_centers, function(center) {
    unlist(spark_invoke(center, "toArray"), recursive = FALSE)
  }))

  names(centers_list) <- as.character(dplyr::tbl_vars(x))
  centers <- as.data.frame(centers_list, stringsAsFactors = FALSE)

  ml_model("kmeans", model, centers = centers)
}
