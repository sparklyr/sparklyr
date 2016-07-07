#' Spark ML -- K-Means Clustering
#'
#' Perform k-means clustering on a \code{spark_tbl}.
#'
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#' @param centers The number of cluster centers to compute.
#' @param iter.max Maximum number of iterations allowed.
#' @param features Which columns to use in the k-means fit. Defaults to
#'   all columns within \code{x}.
#' @param ... Optional arguments; currently unused.
#' 
#' @seealso For information on how Spark k-means clustering is implemented, please see
#'   \url{http://spark.apache.org/docs/latest/mllib-clustering.html#k-means}.
#'
#' @family Spark ML routines
#'
#' @export
ml_kmeans <- function(x,
                      centers,
                      iter.max = 10,
                      features = dplyr::tbl_vars(x),
                      ...) {
  
  df <- spark_dataframe(x)
  sc <- spark_connection(df)
  
  prepare_features(features)
  
  centers <- ensure_scalar_integer(centers)
  iter.max <- ensure_scalar_integer(iter.max)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)
  
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, envir = envir)

  # invoke KMeans
  kmeans <- invoke_new(
    sc,
    "org.apache.spark.ml.clustering.KMeans"
  )

  model <- kmeans %>%
    invoke("setK", centers) %>%
    invoke("setMaxIter", iter.max) %>%
    invoke("setFeaturesCol", envir$features)
  
  if (only_model) return(model)
  
  fit <- model %>%
    invoke("fit", tdf)

  # extract cluster centers
  kmmCenters <- invoke(fit, "clusterCenters")

  centersList <- transpose_list(lapply(kmmCenters, function(center) {
    as.numeric(invoke(center, "toArray"))
  }))

  names(centersList) <- features
  centers <- as.data.frame(centersList, stringsAsFactors = FALSE)

  ml_model("kmeans", fit,
           centers = centers,
           features = features,
           data = df,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_kmeans <- function(x, ...) {
  
  preamble <- sprintf(
    "K-means clustering with %s %s",
    nrow(x$centers),
    if (nrow(x$centers) == 1) "cluster" else "clusters"
  )
  
  cat(preamble, sep = "\n")
  print_newline()
  ml_model_print_centers(x)

}

#' @export
#' @importFrom stats predict
fitted.ml_model_kmeans <- function(object, ...) {
  predict(object)
}
