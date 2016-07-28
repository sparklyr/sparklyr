#' Spark ML -- K-Means Clustering
#'
#' Perform k-means clustering on a Spark DataFrame.
#'
#' @template roxlate-ml-x
#' @param centers The number of cluster centers to compute.
#' @template roxlate-ml-max-iter
#' @template roxlate-ml-features
#' @template roxlate-ml-dots
#'
#' @seealso For information on how Spark k-means clustering is implemented, please see
#'   \url{http://spark.apache.org/docs/latest/mllib-clustering.html#k-means}.
#'
#' @family Spark ML routines
#'
#' @export
ml_kmeans <- function(x,
                      centers,
                      max.iter = 100,
                      features = dplyr::tbl_vars(x),
                      ...) {

  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  ml_prepare_features(df, features)

  centers <- ensure_scalar_integer(centers)
  max.iter <- ensure_scalar_integer(max.iter)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, envir = envir)

  envir$model <- "org.apache.spark.ml.clustering.KMeans"
  kmeans <- invoke_new(sc, envir$model)

  model <- kmeans %>%
    invoke("setK", centers) %>%
    invoke("setMaxIter", max.iter) %>%
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
  centers <- as.data.frame(centersList, stringsAsFactors = FALSE, optional = TRUE)

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
fitted.ml_model_kmeans <- function(object, ...) {
  predict(object)
}
