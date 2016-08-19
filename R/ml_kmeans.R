#' Spark ML -- K-Means Clustering
#'
#' Perform k-means clustering on a Spark DataFrame.
#'
#' @template roxlate-ml-x
#' @param centers The number of cluster centers to compute.
#' @template roxlate-ml-max-iter
#' @template roxlate-ml-features
#' @template roxlate-ml-dots
#' @template roxlate-ml-compute-cost
#' @template roxlate-ml-tol
#'
#' @seealso For information on how Spark k-means clustering is implemented, please see
#'   \url{http://spark.apache.org/docs/latest/mllib-clustering.html#k-means}.
#'
#' @family Spark ML routines
#'
#' @references Bahmani et al., Scalable K-Means++, VLDB 2012
#'
#' @return \link{ml_model} object of class \code{kmeans} with overloaded \code{print}, \code{fitted} and \code{predict} functions.
#'
#' @export
ml_kmeans <- function(x,
                      centers,
                      max.iter = 100,
                      features = dplyr::tbl_vars(x),
                      compute.cost = TRUE,
                      tolerance = 0.0001,
                      ...) {

  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  df <- ml_prepare_features(df, features)

  centers <- ensure_scalar_integer(centers)
  max.iter <- ensure_scalar_integer(max.iter)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)
  tolerance <- ensure_scalar_double(tolerance)

  envir <- new.env(parent = emptyenv())

  envir$id <- random_string("id_")
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, envir = envir)

  envir$model <- "org.apache.spark.ml.clustering.KMeans"
  kmeans <- invoke_new(sc, envir$model)

  model <- kmeans %>%
    invoke("setK", centers) %>%
    invoke("setMaxIter", max.iter) %>%
    invoke("setTol", tolerance) %>%
    invoke("setFeaturesCol", envir$features)


  if (only_model) return(model)

  fit <- model %>%
    invoke("fit", tdf)

  # extract cluster centers
  kmmCenters <- invoke(fit, "clusterCenters")

  # compute cost for k-means
  if (compute.cost)
    kmmCost <- invoke(fit, "computeCost", tdf)

  centersList <- transpose_list(lapply(kmmCenters, function(center) {
    as.numeric(invoke(center, "toArray"))
  }))

  names(centersList) <- features
  centers <- as.data.frame(centersList, stringsAsFactors = FALSE, optional = TRUE)

  ml_model("kmeans", fit,
           centers = centers,
           features = features,
           data = df,
           model.parameters = as.list(envir),
           cost = ifelse(compute.cost, kmmCost, NULL)
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

  print_newline()
  cat("Within Set Sum of Squared Errors = ",
      if (is.null(x$cost)) "not computed." else x$cost
  )

}

#' @export
fitted.ml_model_kmeans <- function(object, ...) {
  predict(object)
}
