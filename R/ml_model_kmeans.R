new_ml_model_kmeans <- function(pipeline, pipeline_model, model,
                                dataset, formula, feature_names,
                                call) {
  summary <- model$summary

  centers <- model$cluster_centers() %>%
    do.call(rbind, .) %>%
    as.data.frame() %>%
    rlang::set_names(feature_names)

  cost <- try_null(
    pipeline_model %>%
      ml_stage(1) %>%
      ml_transform(dataset) %>%
      model$compute_cost()
  )
  new_ml_model_clustering(
    pipeline, pipeline_model,
    model, dataset, formula,
    centers = centers,
    cost = cost,
    summary = summary,
    subclass = "ml_model_kmeans",
    .features = feature_names
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

#' @rdname ml_kmeans
#' @param model A fitted K-means model returned by \code{ml_kmeans()}
#' @param dataset Dataset on which to calculate K-means cost
#' @return \code{ml_compute_cost()} returns the K-means cost (sum of
#'   squared distances of points to their nearest center) for the model
#'   on the given data.
#' @export
ml_compute_cost <- function(model, dataset) {
  spark_require_version(spark_connection(spark_jobj(model)), "2.0.0")

  if (inherits(model, "ml_model_kmeans")) {
    model$pipeline_model %>%
      ml_stage(1) %>%
      ml_transform(dataset) %>%
      model$model$compute_cost()
  } else
    model$compute_cost(dataset)
}
