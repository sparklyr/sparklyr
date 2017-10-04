ml_is_instance_of <- function(jobj, type) {
  sc <- spark_connection(jobj)
  invoke_static(sc, "java.lang.Class", "forName",
                paste0("org.apache.spark.ml.", type)) %>%
    invoke("isInstance", jobj)
}

ml_ancestry <- function(jobj) {
  # TODO optimize
  classes <- c("feature.CountVectorizer", "feature.CountVectorizerModel",
               "classification.LogisticRegression",
               "classification.LogisticRegressionModel",
               "regression.LinearRegression", "regression.LinearRegressionModel",
               "regression.GeneralizedLinearRegression",
               "regression.GeneralizedLinearRegressionModel",
               "regression.DecisionTreeRegressor", "regression.DecisionTreeRegressionModel",
               "classification.DecisionTreeClassifier", "classification.DecisionTreeClassificationModel",
               "tuning.CrossValidator",
               "Pipeline", "PipelineModel",
               "Estimator", "Transformer")

  Filter(function(x) ml_is_instance_of(jobj, x),
         classes)
}

ml_package <- function(jobj) {
  jobj_info(jobj)$class %>%
    strsplit("\\.") %>%
    rlang::flatten_chr() %>%
    dplyr::nth(-2L)
}

ml_constructor_dispatch <- function(jobj) {
  # TODO consider mapping using hash table
  switch(ml_ancestry(jobj)[1],
         "feature.CountVectorizer" = new_ml_count_vectorizer(jobj),
         "feature.CountVectorizerModel" = new_ml_count_vectorizer_model(jobj),
         "classification.LogisticRegressionModel" = new_ml_logistic_regression_model(jobj),
         "classification.LogisticRegression" = new_ml_logistic_regression(jobj),
         "regression.LinearRegression" = new_ml_linear_regression(jobj),
         "regression.LinearRegressionModel" = new_ml_linear_regression_model(jobj),
         "regression.GeneralizedLinearRegression" = new_ml_generalized_linear_regression(jobj),
         "regression.GeneralizedLinearRegressionModel" = new_ml_generalized_linear_regression_model(jobj),
         "regression.DecisionTreeRegressor" = new_ml_decision_tree_regressor(jobj),
         "regression.DecisionTreeRegressionModel" = new_ml_decision_tree_regression_model(jobj),
         "Pipeline" = new_ml_pipeline(jobj),
         "PipelineModel" = new_ml_pipeline_model(jobj),
         "Transformer" = new_ml_transformer(jobj),
         "Estimator" = new_ml_estimator(jobj),
         "tuning.CrossValidator" = new_ml_cross_validator(jobj),
         new_ml_pipeline_stage(jobj))
}

new_ml_pipeline_stage <- function(jobj, ..., subclass = NULL) {
  structure(
    list(
      uid = invoke(jobj, "uid"),
      type = jobj_info(jobj)$class,
      param_map = ml_get_param_map(jobj),
      ...,
      .jobj = jobj
    ),
    class = c(subclass, "ml_pipeline_stage")
  )
}
