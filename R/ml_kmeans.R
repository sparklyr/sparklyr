spark_ml_kmeans <- function(x, centers, iter.max = 10,
                            features = dplyr::tbl_vars(x))
{
  scon <- spark_scon(x)
  df <- as_spark_dataframe(x)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, envir = envir)

  # invoke KMeans
  kmeans <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.clustering.KMeans"
  )

  fit <- kmeans %>%
    spark_invoke("setK", as.integer(centers)) %>%
    spark_invoke("setMaxIter", as.integer(iter.max)) %>%
    spark_invoke("setFeaturesCol", envir$features) %>%
    spark_invoke("fit", tdf)


  # extract cluster centers
  kmmCenters <- spark_invoke(fit, "clusterCenters")

  centersList <- transpose_list(lapply(kmmCenters, function(center) {
    as.numeric(spark_invoke(center, "toArray"))
  }))

  names(centersList) <- features
  centers <- as.data.frame(centersList, stringsAsFactors = FALSE)

  ml_model("kmeans", fit,
    centers = centers,
    features = features,
    model.parameters = as.list(envir)
  )
}

#' Computes kmeans from a dplyr source
#' @export
#' @param x A dplyr source.
#' @param centers Number of centers to compute.
#' @param iter.max Maximum number of iterations used to compute kmeans.
#' @param features Which columns to use in the kmeans fit. Defaults to
#'   all columns within \code{x}.
ml_kmeans <- function(x, centers, iter.max = 10, features = dplyr::tbl_vars(x)) {
  spark_ml_kmeans(x, centers, iter.max, features)
}
