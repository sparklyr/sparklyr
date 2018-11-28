new_ml_model_prediction <- function(pipeline_model, dataset, label_col, features_col,
                                    ..., class = character()) {

  feature_names <- ml_feature_names_metadata(pipeline_model, dataset, features_col)

  new_ml_model(
    pipeline_model,
    dataset = dataset,
    label_col = label_col,
    features_col = features_col,
    feature_names = feature_names,
    ...,
    class = c(class, "ml_model_prediction")
  )
}

#' Constructors for `ml_model` Objects
#'
#' Functions for developers writing extensions for Spark ML. These functions are constructors
#'   for `ml_model` objects that are returned when using the formula interface.
#'
#' @name ml-model-constructors
#'
#' @export
#' @keywords internal
new_ml_model <- function(pipeline_model, ..., class = character()) {

  sc <- spark_connection(pipeline_model)

  stages <- ml_stages(pipeline_model)
  predictor <- stages[[length(stages)]]

  # for pipeline, fix data prep transformation but use the un-fitted estimator predictor
  pipeline <- stages %>%
    head(-1) %>%
    rlang::invoke(ml_pipeline, ., uid = ml_uid(pipeline_model)) %>%
    ml_add_stage(predictor)

  # workaround for https://issues.apache.org/jira/browse/SPARK-19953
  model_uid <- if (spark_version(sc) < "2.2.0") {
    switch(
      class(predictor)[[1]],
      ml_random_forest_regressor = "rfr",
      ml_random_forest_classifier = "rfc",
      ml_uid(predictor)
    )
  } else {
    ml_uid(predictor)
  }

  model <- ml_stage(pipeline_model, model_uid)

  structure(
    list(
      pipeline_model = pipeline_model,
      pipeline = pipeline,
      model = model,
      ...
    ),
    class = c(class, "ml_model")
  )
}
