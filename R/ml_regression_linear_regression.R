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
  uid = random_string("linear_regression_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...
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
  uid = random_string("linear_regression_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...) {

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
  uid = random_string("linear_regression_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_linear_regression.tbl_spark <- function(
  x,
  formula = NULL,
  response = NULL,
  features = NULL,
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

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    # formula <- (if (rlang::is_formula(formula)) rlang::expr_text else identity)(formula)
    sc <- spark_connection(x)
    r_formula <- ft_r_formula(sc, formula, features_col,
                              label_col,
                              dataset = x)
    pipeline <- ml_pipeline(r_formula, predictor)

    pipeline_model <- pipeline %>%
      ml_fit(x)

    new_ml_model_linear_regression(
      pipeline,
      pipeline_model,
      predictor$uid,
      formula,
      dataset = x)
  }
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

new_ml_linear_regression_model <- function(jobj) {
  summary <- if (invoke(jobj, "hasSummary"))
    new_ml_summary_linear_regression_model(invoke(jobj, "summary"), solver = invoke(jobj, "solver"))
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

new_ml_summary_linear_regression_model <- function(jobj, solver) {
  is_normal_solver <- identical(solver, "normal")
  new_ml_summary(
    jobj,
    coefficient_standard_errors = if (is_normal_solver)
      invoke(jobj, "coefficientStandardErrors") else NA,
    degrees_of_freedom = if (spark_version(spark_connection(jobj)) >= "2.2.0")
      invoke(jobj, "degreesOfFreedom") else NA,
    deviance_residuals = invoke(jobj, "devianceResiduals"),
    explained_variance = invoke(jobj, "explainedVariance"),
    features_col = invoke(jobj, "featuresCol"),
    label_col = invoke(jobj, "labelCol"),
    mean_absolute_error = invoke(jobj, "meanAbsoluteError"),
    mean_squared_error = invoke(jobj, "meanSquaredError"),
    num_instances = invoke(jobj, "numInstances"),
    p_values = if (is_normal_solver) invoke(jobj, "pValues") else NA,
    prediction_col = invoke(jobj, "predictionCol"),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    r2 = invoke(jobj, "r2"),
    residuals = invoke(jobj, "residuals") %>% sdf_register(),
    root_mean_squared_error = invoke(jobj, "rootMeanSquaredError"),
    t_values = if (is_normal_solver) invoke(jobj, "tValues") else NA,
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
    names(coefficients) <- feature_names

    coefficients <- if (ml_param(model, "fit_intercept"))
      rlang::set_names(
        c(invoke(jobj, "intercept"), model$coefficients),
        c("(Intercept)", feature_names))

  call <- rlang::ctxt_frame(rlang::ctxt_frame()$caller_pos)$expr

  summary <- model$summary

  new_ml_model_regression(
    pipeline, pipeline_model, model_uid, formula, dataset,
    coefficients = coefficients,
    summary = summary,
    subclass = "ml_model_linear_regression",
    .call = call
  )
}

# Generic implementations

#' @export
ml_fit.ml_linear_regression <- function(x, data, ...) {
  jobj <- spark_jobj(x) %>%
    invoke("fit", spark_dataframe(data))
  new_ml_linear_regression_model(jobj)
}
