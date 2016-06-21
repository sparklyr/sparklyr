#' Spark ML -- K-Means Clustering
#'
#' Perform k-means clustering on a \code{spark_tbl}.
#'
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#' @param centers The number of cluster centers to compute.
#' @param iter.max Maximum number of iterations allowed.
#' @param features Which columns to use in the k-means fit. Defaults to
#'   all columns within \code{x}.
#'
#' @seealso For information on how Spark k-means clustering is implemented, please see
#'   \url{http://spark.apache.org/docs/latest/mllib-clustering.html#k-means}.
#'
#' @family Spark ML routines
#'
#' @export
ml_kmeans <- function(x, centers, iter.max = 10, features = dplyr::tbl_vars(x)) {
  
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)
  
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, envir = envir)

  # invoke KMeans
  kmeans <- spark_invoke_new(
    sc,
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
