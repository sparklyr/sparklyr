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
               "classification.DecisionTreeClassifier", "classification.DecisionTreeClassificationModel",
               "classification.GBTClassifier", "classification.GBTClassificationModel",
               "classification.RandomForestClassifier",
               "classification.RandomForestClassificationModel",
               "classification.NaiveBayes", "classification.NaiveBayesModel",
               "classification.MultilayerPerceptronClassifier",
               "classification.MultilayerPerceptronClassificationModel",
               "classification.OneVsRest", "classification.OneVsRestModel",
               "regression.LinearRegression", "regression.LinearRegressionModel",
               "regression.GeneralizedLinearRegression",
               "regression.GeneralizedLinearRegressionModel",
               "regression.DecisionTreeRegressor", "regression.DecisionTreeRegressionModel",
               "regression.GBTRegressor", "regression.GBTRegressionModel",
               "regression.RandomForestRegressor", "regression.RandomForestRegressionModel",
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
         "classification.GBTClassifier" = new_ml_gbt_classifier(jobj),
         "classification.GBTClassificationModel" = new_ml_gbt_classification_model(jobj),
         "classification.RandomForestClassifier" = new_ml_random_forest_classifier(jobj),
         "classification.RandomForestClassificationModel" = new_ml_random_forest_classification_model(jobj),
         "classification.NaiveBayes" = new_ml_naive_bayes(jobj),
         "classification.NaiveBayesModel" = new_ml_naive_bayes_model(jobj),
         "classification.MultilayerPerceptronClassifier" = new_ml_multilayer_perceptron_classifier(jobj),
         "classification.MultilayerPerceptronClassificationModel" = new_ml_multilayer_perceptron_classification_model(jobj),
         "classification.OneVsRest" = new_ml_one_vs_rest(jobj),
         "classification.OneVsRestModel" = new_ml_one_vs_rest_model(jobj),
         "regression.LinearRegression" = new_ml_linear_regression(jobj),
         "regression.LinearRegressionModel" = new_ml_linear_regression_model(jobj),
         "regression.GeneralizedLinearRegression" = new_ml_generalized_linear_regression(jobj),
         "regression.GeneralizedLinearRegressionModel" = new_ml_generalized_linear_regression_model(jobj),
         "regression.DecisionTreeRegressor" = new_ml_decision_tree_regressor(jobj),
         "regression.DecisionTreeRegressionModel" = new_ml_decision_tree_regression_model(jobj),
         "regression.GBTRegressor" = new_ml_gbt_regressor(jobj),
         "regression.GBTRegressionModel" = new_ml_gbt_regression_model(jobj),
         "regression.RandomForestRegressor" = new_ml_random_forest_regressor(jobj),
         "regression.RandomForestRegressionModel" = new_ml_random_forest_regression_model(jobj),
         "regression.AFTSurvivalRegression" = new_ml_aft_survival_regression(jobj),
         "regression.AFTSurvivalRegressionModel" = new_ml_aft_survival_regression_model(jobj),
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
