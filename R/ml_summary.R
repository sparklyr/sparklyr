new_ml_summary <- function(jobj, ..., subclass = NULL) {
  structure(
    list(
      type = jobj_info(jobj)$class,
      ...,
      .jobj = jobj
    ),
    class = c(subclass, "ml_summary")
  )
}

new_ml_summary_clustering <- function(jobj, ..., subclass = NULL) {
  new_ml_summary(
    jobj,
    cluster = function() invoke(jobj, "cluster") %>% sdf_register(), # lazy val
    cluster_sizes = function() invoke(jobj, "clusterSizes"), # lazy val
    features_col = invoke(jobj, "featuresCol"),
    k = invoke(jobj, "k"),
    prediction_col = invoke(jobj, "predictionCol"),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    subclass = c(subclass, "ml_summary_clustering")
  )
}
