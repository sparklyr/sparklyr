#' @export
ml_linear_regression <- function(
  x,
  features_col = "features",
  label_col = "label",
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  weight_col = NULL,
  prediction_col = "prediction",
  solver = "auto",
  standardization = TRUE,
  tol = 1e-6,
  uid = random_string("linear_regression_"), ...
) {
  UseMethod("ml_linear_regression")
}

#' @export
ml_linear_regression.spark_connection <- function(
  x,
  features_col = "features",
  label_col = "label",
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  weight_col = NULL,
  prediction_col = "prediction",
  solver = "auto",
  standardization = TRUE,
  tol = 1e-6,
  uid = random_string("linear_regression_"), ...) {

  ml_validate_args()

  jobj <- ml_new_regressor(
    x, "org.apache.spark.ml.regression.LinearRegression", uid,
    features_col, label_col, prediction_col
  ) %>%
    invoke("setElasticNetParam", elastic_net_param) %>%
    invoke("setFitIntercept", fit_intercept) %>%
    invoke("setRegParam", reg_param) %>%
    invoke("setMaxIter", max_iter) %>%
    invoke("setSolver", solver) %>%
    invoke("setStandardization", standardization) %>%
    invoke("setTol", tol)

  if (!is.null(weight_col))
    jobj <- invoke(jobj, "setWeightCol", weight_col)

  new_ml_linear_regression(jobj)
}

#' @export
ml_linear_regression.ml_pipeline <- function(
  x,
  features_col = "features",
  label_col = "label",
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  weight_col = NULL,
  prediction_col = "prediction",
  solver = "auto",
  standardization = TRUE,
  tol = 1e-6,
  uid = random_string("linear_regression_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

# Validator
ml_validator_linear_regression <- function(args, nms) {
  old_new_mapping <- list(
    intercept = "fit_intercept",
    alpha = "elastic_net_param",
    lambda = "reg_param",
    weights.column = "weight_col",
    iter.max = "max_iter",
    max.iter = "max_iter"
  )

  ml_apply_validation(
    {
      elastic_net_param <- ensure_scalar_double(elastic_net_param)
      reg_param <- ensure_scalar_double(reg_param)
      max_iter <- ensure_scalar_integer(max_iter)
      fit_intercept <- ensure_scalar_boolean(fit_intercept)
      standardization <- ensure_scalar_boolean(standardization)
      tol <- ensure_scalar_double(tol)
      solver <- rlang::arg_match(solver, c("auto", "l-bfgs", "normal"))
      if (!is.null(weight_col))
        weight_col <- ensure_scalar_character(weight_col)
    },
    args, nms, old_new_mapping
  )
}

# Constructors

new_ml_linear_regression <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_linear_regression")
}

new_ml_logistic_regression_model <- function(jobj) {
  summary <- if (invoke(jobj, "hasSummary"))
    new_ml_summary_linear_regression_model(invoke(jobj, "summary"))
  else NA

  new_ml_prediction_model(
    jobj,
    coefficients = read_spark_vector(jobj, "coefficients"),
    intercept = invoke(jobj, "intercept"),
    num_features = invoke(jobj, "numFeatures"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    summary = summary,
    subclass = "ml_linear_regression_model")
}

new_ml_summary_linear_regression_model <- function(jobj) {
  new_ml_summary(
    jobj,
    coefficient_standard_errors = invoke(jobj, "coefficientStandardErrors"),
    degrees_of_freedom = invoke(jobj, "degreesOfFreedom"),
    deviance_residuals = invoke(jobj, "devianceResiduals"),
    explained_variance = invoke(jobj, "explainedVariance"),
    features_col = invoke(jobj, "featuresCol"),
    label_col = invoke(jobj, "labelCol"),
    mean_absolute_error = invoke(jobj, "meanAbsoluteError"),
    mean_squared_error = invoke(jobj, "meanSquaredError"),
    num_instances = invoke(jobj, "numInstances"),
    p_values = invoke(jobj, "pValues"),
    prediction_col = invoke(jobj, "predictionCol"),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    r2 = invoke(jobj, "r2"),
    residuals = invoke(jobj, "residuals") %>% sdf_register(),
    root_mean_squared_error = invoke(jobj, "rootMeanSquaredError"),
    t_values = invoke(jobj, "tValues"),
    subclass = "ml_summary_linear_regression")
}

new_ml_model_linear_regression <- function(
  pipeline, pipeline_model, model_uid, formula, dataset, .call) {

  model <- pipeline_model %>%
    ml_stage(model_uid)
  jobj <- spark_jobj(model)
  sc <- spark_connection(model)
  features_col <- ml_param(model, "features_col")
  label_col <- ml_param(model, "label_col")
  transformed_tbl <- pipeline_model %>%
    ml_transform(dataset)

  feature_names <- ml_column_metadata(transformed_tbl, features_col) %>%
    `[[`("attrs") %>%
    `[[`("numeric") %>%
    dplyr::pull("name")

    coefficients <- model$coefficients
    colnames(coefficients) <- feature_names

    coefficients <- if (ml_param(model, "fit_intercept"))
      rlang::set_names(
        c(invoke(jobj, "intercept"), model$coefficients),
        c("(Intercept)", feature_names))

  call <- rlang::ctxt_frame(rlang::ctxt_frame()$caller_pos)$expr

  summary <- model$summary

  new_ml_model_classification(
    pipeline, pipeline_model, model_uid, formula, dataset,
    coefficients = coefficients,
    summary = summary,
    subclass = "ml_model_linear_regression",
    .call = call
  )
}
