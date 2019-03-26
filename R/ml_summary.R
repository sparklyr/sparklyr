new_ml_summary <- function(jobj, ..., class = character()) {
  structure(
    list(
      type = jobj_info(jobj)$class,
      ...,
      .jobj = jobj
    ),
    class = c(class, "ml_summary")
  )
}

#' @export
print.ml_summary <- function(x, ...) {
  short_type <- strsplit(x$type, "\\.") %>%
    unlist() %>%
    dplyr::last()

  cat(short_type, "\n")
  cat(" Access the following via `$` or `ml_summary()`.", "\n")
  for (m in setdiff(names(x), c("type", ".jobj"))) {
    print_value <- if (is.function(x[[m]])) paste0(m, "()") else m
    cat(" -", print_value, "\n")
  }
}

new_ml_clustering_summary <- function(jobj, ..., class = character()) {
  new_ml_summary(
    jobj,
    cluster = function() invoke(jobj, "cluster") %>% sdf_register(), # lazy val
    cluster_sizes = function() invoke(jobj, "clusterSizes"), # lazy val
    features_col = invoke(jobj, "featuresCol"),
    k = invoke(jobj, "k"),
    prediction_col = invoke(jobj, "predictionCol"),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    ...,
    class = c(class, "ml_clustering_summary")
  )
}
