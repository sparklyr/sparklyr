#' @export
ml_generalized_linear_regression <- function(
  x,
  family = "gaussian",
  features_col = "features",
  label_col = "label",
  fit_intercept = TRUE,
  link = NULL,
  link_power = NULL,
  link_prediction_col = NULL,
  reg_param = 0,
  max_iter = 25L,
  weight_col = NULL,
  prediction_col = "prediction",
  solver = "irls",
  tol = 1e-6,
  variance_power = 0,
  uid = random_string("generalized_linear_regression_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...
) {
  spark_require_version(spark_connection(x), "2.0.0")
  UseMethod("ml_generalized_linear_regression")
}

#' @export
ml_generalized_linear_regression.spark_connection <- function(
  x,
  family = "gaussian",
  features_col = "features",
  label_col = "label",
  fit_intercept = TRUE,
  link = NULL,
  link_prediction_col = NULL,
  reg_param = 0,
  max_iter = 25L,
  weight_col = NULL,
  prediction_col = "prediction",
  solver = "irls",
  tol = 1e-6,
  variance_power = 0,
  uid = random_string("generalized_linear_regression_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...) {

  ml_ratify_args()

  jobj <- ml_new_regressor(
    x, "org.apache.spark.ml.regression.GeneralizedLinearRegression", uid,
    features_col, label_col, prediction_col
  ) %>%
    invoke("setFamily", family) %>%
    invoke("setFitIntercept", fit_intercept) %>%
    invoke("setRegParam", reg_param) %>%
    invoke("setMaxIter", max_iter) %>%
    invoke("setSolver", solver) %>%
    invoke("setTol", tol)

  if (identical(family, "tweedie")) {
    jobj <- jobj %>%
      invoke("setLinkPower", link_power) %>%
      invoke("setVariancePower", variance_power)
  }

  if (!rlang::is_null(link))
    jobj <- invoke(jobj, "setLink", link)

  if (!rlang::is_null(link_prediction_col))
    jobj <- invoke(jobj, "setLinkPredictionCol", link_prediction_col)

  if (!rlang::is_null(weight_col))
    jobj <- invoke(jobj, "setWeightCol", weight_col)

  new_ml_generalized_linear_regression(jobj)
}

#' @export
ml_generalized_linear_regression.ml_pipeline <- function(
  x,
  family = "gaussian",
  features_col = "features",
  label_col = "label",
  fit_intercept = TRUE,
  link = NULL,
  link_prediction_col = NULL,
  reg_param = 0,
  max_iter = 25L,
  weight_col = NULL,
  prediction_col = "prediction",
  solver = "irls",
  tol = 1e-6,
  variance_power = 0,
  uid = random_string("generalized_linear_regression_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_generalized_linear_regression.tbl_spark <- function(
  x,
  family = "gaussian",
  features_col = "features",
  label_col = "label",
  fit_intercept = TRUE,
  link = NULL,
  link_prediction_col = NULL,
  reg_param = 0,
  max_iter = 25L,
  weight_col = NULL,
  prediction_col = "prediction",
  solver = "irls",
  tol = 1e-6,
  variance_power = 0,
  uid = random_string("generalized_linear_regression_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...) {

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

    new_ml_model_generalized_linear_regression(
      pipeline = pipeline,
      pipeline_model = pipeline_model,
      model = pipeline_model %>%
        ml_stage(2),
      dataset = x,
      formula = formula)
  }
}

# Validator
ml_validator_generalized_linear_regression <- function(args, nms) {
  old_new_mapping <- list(
    intercept = "fit_intercept",
    weights.column = "weight_col",
    iter.max = "max_iter",
    max.iter = "max_iter"
  )

  args %>%
    ml_apply_validation({
      reg_param <- ensure_scalar_double(reg_param)
      # TODO bounds on regularization parameters
      max_iter <- ensure_scalar_integer(max_iter)
      if (is.function(family)) {
        family <- family()
        link <- ensure_scalar_character(family$link)
        family <- ensure_scalar_character(family$family)
      } else if (rlang::is_character(family)) {
        family <- rlang::arg_match(family, c(
          "gaussian", "binomial", "poisson", "gamma", "tweedie"))
        link <- ensure_scalar_character(link, allow.null = TRUE)
      } else {
        link <- ensure_scalar_character(family$link)
        family <- ensure_scalar_character(family$family)
      }

      # TODO check family-link compatibility on R side
      fit_intercept <- ensure_scalar_boolean(fit_intercept)
      solver <- rlang::arg_match(solver, c("irls"))
      tol <- ensure_scalar_double(tol)
      if (!rlang::is_null(weight_col))
        weight_col <- ensure_scalar_character(weight_col)
      if (!rlang::is_null(link_prediction_col))
        link_prediction_col <- ensure_scalar_character(link_prediction_col)
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_generalized_linear_regression <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_generalized_linear_regression")
}

new_ml_generalized_linear_regression_model <- function(jobj) {
  summary <- if (invoke(jobj, "hasSummary"))
  {
    new_ml_summary_generalized_linear_regression_model(invoke(jobj, "summary"))
  } else NA

  new_ml_prediction_model(
    jobj,
    coefficients = read_spark_vector(jobj, "coefficients"),
    intercept = invoke(jobj, "intercept"),
    num_features = invoke(jobj, "numFeatures"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    link_prediction_col = if (invoke(jobj, "isSet", invoke(jobj, "linkPredictionCol"))) invoke(jobj, "getLinkPredictionCol") else NULL,
    summary = summary,
    subclass = "ml_generalized_linear_regression_model")
}

new_ml_summary_generalized_linear_regression_model <- function(jobj) {
  version <- jobj %>%
    spark_connection() %>%
    spark_version()
  resid <- function(x) invoke(jobj, "residuals", x) %>%
    sdf_register()

  new_ml_summary(
    jobj,
    aic = invoke(jobj, "aic"),
    coefficient_standard_errors = try_null(invoke(jobj, "coefficientStandardErrors")),
    degrees_of_freedom = invoke(jobj, "degreesOfFreedom"),
    deviance = invoke(jobj, "deviance"),
    dispersion = invoke(jobj, "dispersion"),
    null_deviance = invoke(jobj, "nullDeviance"),
    num_instances = if (version > "2.2.0") invoke(jobj, "numInstances") else NULL,
    num_iterations = try_null(invoke(jobj, "numIterations")),
    p_values = try_null(invoke(jobj, "pValues")),
    prediction_col = invoke(jobj, "predictionCol"),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    rank = invoke(jobj, "rank"),
    residual_degree_of_freedom = invoke(jobj, "residualDegreeOfFreedom"),
    residual_degree_of_freedom_null = invoke(jobj, "residualDegreeOfFreedomNull"),
    residuals = function(type = "deviance") (invoke(jobj, "residuals", type)
                                             %>% sdf_register()),
    solver = try_null(invoke(Jobj, "solver")),
    t_values = try_null(invoke(jobj, "tValues")),
    subclass = "ml_summary_generalized_linear_regression")
}

new_ml_model_generalized_linear_regression <- function(
  pipeline, pipeline_model, model, dataset, formula) {

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
    pipeline, pipeline_model, model, dataset, formula,
    coefficients = coefficients,
    summary = summary,
    subclass = "ml_model_generalized_linear_regression",
    .response = gsub("~.+$", "", formula) %>% trimws(),
    .features = feature_names,
    .call = call
  )
}

# Generic implementations

#' @export
ml_fit.ml_generalized_linear_regression <- function(x, data, ...) {
  jobj <- spark_jobj(x) %>%
    invoke("fit", spark_dataframe(data))
  new_ml_generalized_linear_regression_model(jobj)
}

#' @export
print.ml_model_generalized_linear_regression <-
  function(x, digits = max(3L, getOption("digits") - 3L), ...)
  {
    ml_model_print_call(x)
    print_newline()
    ml_model_print_coefficients(x)
    print_newline()

    cat(
      sprintf("Degress of Freedom:  %s Total (i.e. Null);  %s Residual",
              x$summary$residual_degree_of_freedom_null,
              x$summary$residual_degree_of_freedom),
      sep = "\n"
    )
    cat(sprintf("Null Deviance:       %s", signif(x$summary$null_deviance, digits)), sep = "\n")
    cat(sprintf("Residual Deviance:   %s\tAIC: %s",
                signif(x$summary$deviance, digits),
                signif(x$summary$aic, digits)), sep = "\n")
  }

#' @export
summary.ml_model_generalized_linear_regression <-
  function(object, digits = max(3L, getOption("digits") - 3L), ...)
  {
    ml_model_print_call(object)
    print_newline()
    ml_model_print_residuals(object, residuals.header = "Deviance Residuals")
    print_newline()
    ml_model_print_coefficients_detailed(object)
    print_newline()

    printf("(Dispersion paramter for %s family taken to be %s)\n\n",
           ml_param(ml_stage(object$pipeline_model, 2), "family"),
           signif(object$summary$dispersion, digits + 3))

    printf("   Null  deviance: %s on %s degress of freedom\n",
           signif(object$summary$null_deviance, digits + 2),
           signif(object$summary$residual_degree_of_freedom_null, digits))

    printf("Residual deviance: %s on %s degrees of freedom\n",
           signif(object$summary$deviance, digits + 2),
           signif(object$summary$degrees_of_freedom, digits))
    printf("AIC: %s\n", signif(object$summary$aic, digits + 1))

    invisible(object)
  }

#' @export
residuals.ml_model_generalized_linear_regression <- function(
  object,
  type = c("deviance", "pearson", "working", "response"),
  ...) {

  type <- rlang::arg_match(type)
  ensure_scalar_character(type)

  residuals <- object %>%
    `[[`("summary") %>%
    `[[`("residuals") %>%
    do.call(list(type = type))

  sdf_read_column(residuals, paste0(type, "Residuals"))
}

#' @rdname sdf_residuals
#' @param type type of residuals which should be returned.
#' @export
sdf_residuals.ml_model_generalized_linear_regression <- function(
  object,
  type = c("deviance", "pearson", "working", "response"),
  ...) {

  type <- rlang::arg_match(type)
  ensure_scalar_character(type)

  residuals <- object %>%
    `[[`("summary") %>%
    `[[`("residuals") %>%
    do.call(list(type = type)) %>%
    dplyr::rename(residuals = !!rlang::sym(paste0(type, "Residuals")))

  ml_model_data(object) %>%
    sdf_fast_bind_cols(residuals)
}

#' @export
sdf_predict.ml_model_generalized_linear_regression <- function(object, newdata, ...) {
  # when newdata is not supplied, attempt to use original dataset
  if (missing(newdata) || is.null(newdata))
    newdata <- object$dataset

  object$pipeline_model %>%
    ml_transform(newdata) %>%
    select(!!!rlang::syms(c(tbl_vars(newdata), "prediction")))
}
