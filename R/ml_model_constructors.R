new_ml_model <- function(pipeline, pipeline_model, model_uid, ..., subclass = NULL) {
  model_params <- pipeline_model$stages %>%
    `[[`(grep(model_uid, pipeline_model$stage_uids)) %>%
    `[[`("param_map")

  structure(
    list(
      pipeline = pipeline,
      pipeline_model = pipeline_model,
      model_params = model_params,
      ...
    ),
    class = c(subclass, "ml_model")
  )
}

new_ml_model_prediction <- function(pipeline, pipeline_model, model_uid, formula, ...,
                                    subclass = NULL) {
  new_ml_model(
    pipeline, pipeline_model, model_uid,
    formula = formula,
    ...,
    subclass = c(subclass, "ml_model_prediction"))
}

new_ml_model_classification <- function(pipeline, pipeline_model, model_uid, formula, ..., subclass = NULL) {
  new_ml_model_prediction(
    pipeline, pipeline_model, model_uid, formula,
    ...,
    subclass = c(subclass, "ml_model_classification"))
}

new_ml_model_logistic_regression <- function(pipeline, pipeline_model, model_uid, formula, dataset) {

  model <- pipeline_model$stages %>%
    `[[`(grep(model_uid, pipeline_model$stage_uids))

  features_col <- model$param_map$features_col
  transformed_sdf <- pipeline_model %>%
    ml_transform(dataset) %>%
    spark_dataframe()
  features_col_index <- transformed_sdf %>%
    invoke("schema") %>%
    invoke("fieldIndex", features_col) %>%
    ensure_scalar_integer()
  feature_names <- transformed_sdf %>%
    invoke("schema") %>%
    invoke("apply", features_col_index) %>%
    invoke("metadata") %>%
  invoke("json") %>%
    jsonlite::fromJSON() %>%
    `[[`("ml_attr") %>%
    `[[`("attrs") %>%
    `[[`("numeric") %>%
    dplyr::pull("name")

  coefficients <- model$.jobj %>%
    sparklyr:::read_spark_vector("coefficients")

  coefficients <- if (model$param_map[["fit_intercept"]])
    rlang::set_names(
      c(invoke(model$.jobj, "intercept"), coefficients),
      c("(Intercept)", feature_names)
    )
  else
    rlang::set_names(coefficients, feature_names)


  new_ml_model_classification(
    pipeline, pipeline_model, model_uid, formula, dataset,
    coefficients = coefficients,
    subclass = "ml_model_logistic_regression"
  )
}
