new_ml_model_bisecting_kmeans <- function(pipeline_model, formula, dataset,
                                          features_col) {
  m <- new_ml_model_clustering(
    pipeline_model = pipeline_model,
    formula = formula,
    dataset = dataset,
    features_col = features_col,
    class = "ml_model_bisecting_kmeans"
  )

  model <- m$model

  m$summary <- model$summary

  m$centers <- model$cluster_centers() %>%
    do.call(rbind, .) %>%
    as.data.frame() %>%
    rlang::set_names(m$feature_names)

  m$cost <- possibly_null(
    ~ pipeline_model %>%
      ml_stage(1) %>%
      ml_transform(dataset) %>%
      model$compute_cost()
  )()

  m
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
