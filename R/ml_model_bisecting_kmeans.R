new_ml_model_bisecting_kmeans <- function(pipeline, pipeline_model, model,
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
    subclass = "ml_model_bisecting_kmeans",
    .features = feature_names
  )
}

#' @export
print.ml_model_bisecting_kmeans <- function(x, ...) {
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
