#' @export
ml_logistic_regression <- function(
  x,
  features_col = "features",
  label_col = "label",
  family = c("auto", "binomial", "multinomial"),
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  weight_col = NULL,
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"), ...
) {
  UseMethod("ml_logistic_regression")
}

#' @export
ml_logistic_regression.spark_connection <- function(
  x,
  features_col = "features",
  label_col = "label",
  family = c("auto", "binomial", "multinomial"),
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  weight_col = NULL,
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"), ...) {

  ml_validate_args(rlang::caller_env())

  jobj <- ml_new_classifier(
    x, "org.apache.spark.ml.classification.LogisticRegression", uid,
    label_col, prediction_col, probability_col, raw_prediction_col
  ) %>%
    invoke("setElasticNetParam", elastic_net_param) %>%
    invoke("setFitIntercept", fit_intercept) %>%
    invoke("setRegParam", reg_param) %>%
    invoke("setMaxIter", max_iter)

  if (!is.null(weight_col))
    jobj <- invoke(jobj, "setWeightCol", weight_col)

  new_ml_logistic_regression(jobj)
}

#' @export
ml_logistic_regression.ml_pipeline <- function(
  x,
  features_col = "features",
  label_col = "label",
  family = c("auto", "binomial", "multinomial"),
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  weight_col = NULL,
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"), ...) {

  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_add_stage(x, transformer)
}

#' @export
ml_logistic_regression.tbl_spark <- function(
  x,
  formula = NULL,
  response = NULL,
  features = NULL,
  features_col = "features",
  label_col = "label",
  family = c("auto", "binomial", "multinomial"),
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  weight_col = NULL,
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"), ...) {

  logistic_regression <- ml_new_stage_modified_args(rlang::call_frame())

  ml_formula_transformation()

  if (is.null(formula)) {
    logistic_regression %>%
      ml_fit(x)
  } else {
    # formula <- (if (rlang::is_formula(formula)) rlang::expr_text else identity)(formula)
    sc <- spark_connection(x)
    r_formula <- ml_r_formula(sc, formula, features_col,
                              label_col, force_index_label = TRUE,
                              dataset = x)
    pipeline <- ml_pipeline(r_formula, logistic_regression)

    pipeline_model <- pipeline %>%
      ml_fit(x)

    new_ml_model_logistic_regression(
      pipeline,
      pipeline_model,
      logistic_regression$uid,
      formula,
      dataset = x)
  }
}

# Constructors

new_ml_logistic_regression <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_logistic_regression")
}

new_ml_logistic_regression_model <- function(jobj) {
  new_ml_prediction_model(jobj, "ml_logistic_regression_model")
}

new_ml_model_logistic_regression <- function(pipeline, pipeline_model, model_uid, formula, dataset,
                                             .call) {

  model <- pipeline_model$stages %>%
    `[[`(grep(model_uid, pipeline_model$stage_uids))

  jobj <- model$.jobj

  sc <- spark_connection(model)

  features_col <- model$param_map$features_col

  transformed_sdf <- pipeline_model %>%
    ml_transform(dataset) %>%
    spark_dataframe()

  feature_names <- transformed_sdf %>%
    invoke("schema") %>%
    invoke("apply", transformed_sdf %>%
             invoke("schema") %>%
             invoke("fieldIndex", features_col) %>%
             ensure_scalar_integer()) %>%
    invoke("metadata") %>%
    invoke("json") %>%
    jsonlite::fromJSON() %>%
    `[[`("ml_attr") %>%
    `[[`("attrs") %>%
    `[[`("numeric") %>%
    dplyr::pull("name")

    # multinomial vs. binomial models have separate APIs for
    # retrieving results
    is_multinomial <- invoke(jobj, "numClasses") > 2

    # extract coefficients (can be either a vector or matrix, depending
    # on binomial vs. multinomial)
    coefficients <- if (is_multinomial) {
      if (spark_version(sc) < "2.1.0") stop("Multinomial regression requires Spark 2.1.0 or higher.")

      # multinomial
      coefficients <- read_spark_matrix(jobj, "coefficientMatrix")
      colnames(coefficients) <- feature_names

      if (model$param_map[["fit_intercept"]]) {
        intercept <- read_spark_vector(jobj, "interceptVector")
        coefficients <- cbind(intercept, coefficients)
        colnames(coefficients) <- c("(Intercept)", feature_names)
      }
      coefficients
    } else {
      # binomial
      coefficients <- jobj %>%
        sparklyr:::read_spark_vector("coefficients")
      coefficients <- if (model$param_map[["fit_intercept"]])
        rlang::set_names(
          c(invoke(jobj, "intercept"), coefficients),
          c("(Intercept)", feature_names)
        )
      else
        rlang::set_names(coefficients, feature_names)
      coefficients
    }

  call <- rlang::ctxt_frame(rlang::ctxt_frame()$caller_pos)$expr

  summary <- if (invoke(model$.jobj, "hasSummary"))
    new_ml_summary_logistic_regression_model(invoke(model$.jobj, "summary"))
  else NA

  new_ml_model_classification(
    pipeline, pipeline_model, model_uid, formula, dataset,
    coefficients = coefficients,
    summary = summary,
    subclass = "ml_model_logistic_regression",
    .call = call
  )
}

new_ml_summary_logistic_regression_model <- function(jobj) {
  new_ml_summary(
    jobj,
    area_under_roc = invoke(jobj, "areaUnderROC"),
    f_measure_by_threshold = invoke(jobj, "fMeasureByThreshold"),
    features_col = invoke(jobj, "featuresCol"),
    label_col = invoke(jobj, "labelCol"),
    objective_history = invoke(jobj, "objectiveHistory"),
    pr = invoke(jobj, "pr"),
    precision_by_threshold = invoke(jobj, "precisionByThreshold"),
    predictions = invoke(jobj, "predictions"),
    probability_col = invoke(jobj, "probabilityCol"),
    recall_by_threshold = invoke(jobj, "recallByThreshold"),
    roc = invoke(jobj, "roc"),
    total_iterations = invoke(jobj, "totalIterations"),
    subclass = "ml_summary_logistic_regression")
}
# Generic implementations

#' @export
print.ml_model_logistic_regression <- function(x, ...) {
  ml_model_print_call(x)
  print_newline()
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}

#' @export
summary.ml_model_logistic_regression <- function(object, ...) {
  ml_model_print_call(object)
  print_newline()
  ml_model_print_coefficients(object)
  print_newline()
}


#' Spark ML -- Logistic Regression
#'
#' Perform logistic regression on a Spark DataFrame.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @template roxlate-ml-intercept
#' @template roxlate-ml-regression-penalty
#' @template roxlate-ml-weights-column
#' @template roxlate-ml-iter-max
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
#' ml_logistic_regression.tbl_spark <- function(x,
#'                                    response,
#'                                    features,
#'                                    intercept = TRUE,
#'                                    alpha = 0,
#'                                    lambda = 0,
#'                                    weights.column = NULL,
#'                                    iter.max = 100L,
#'                                    ml.options = ml_options(),
#'                                    ...)
#' {
#'   ml_backwards_compatibility_api()
#'
#'   df <- spark_dataframe(x)
#'   sc <- spark_connection(df)
#'
#'   categorical.transformations <- new.env(parent = emptyenv())
#'   df <- ml_prepare_response_features_intercept(
#'     x = df,
#'     response = response,
#'     features = features,
#'     intercept = intercept,
#'     envir = environment(),
#'     categorical.transformations = categorical.transformations,
#'     ml.options = ml.options
#'   )
#'
#'   alpha <- ensure_scalar_double(alpha)
#'   lambda <- ensure_scalar_double(lambda)
#'   weights.column <- ensure_scalar_character(weights.column, allow.null = TRUE)
#'   iter.max <- ensure_scalar_integer(iter.max)
#'   only.model <- ensure_scalar_boolean(ml.options$only.model)
#'
#'   envir <- new.env(parent = emptyenv())
#'
#'   envir$id <- ml.options$id.column
#'   df <- df %>%
#'     sdf_with_unique_id(envir$id) %>%
#'     spark_dataframe()
#'
#'   tdf <- ml_prepare_dataframe(df, features, response, ml.options = ml.options, envir = envir)
#'
#'   envir$model <- "org.apache.spark.ml.classification.LogisticRegression"
#'   lr <- invoke_new(sc, envir$model)
#'
#'   model <- lr %>%
#'     invoke("setMaxIter", iter.max) %>%
#'     invoke("setFeaturesCol", envir$features) %>%
#'     invoke("setLabelCol", envir$response) %>%
#'     invoke("setFitIntercept", as.logical(intercept)) %>%
#'     invoke("setElasticNetParam", as.double(alpha)) %>%
#'     invoke("setRegParam", as.double(lambda))
#'
#'   if (!is.null(weights.column)) {
#'     model <- model %>%
#'       invoke("setWeightCol", weights.column)
#'   }
#'
#'   if (only.model)
#'     return(model)
#'
#'   fit <- model %>%
#'     invoke("fit", tdf)
#'
#'   # multinomial vs. binomial models have separate APIs for
#'   # retrieving results
#'   numClasses <- invoke(fit, "numClasses")
#'   isMultinomial <- numClasses > 2
#'
#'   # extract coefficients (can be either a vector or matrix, depending
#'   # on binomial vs. multinomial)
#'   coefficients <- if (isMultinomial) {
#'
#'     if (spark_version(sc) < "2.1.0") stop("Multinomial regression requires Spark 2.1.0 or higher.")
#'
#'     # multinomial
#'     coefficients <- read_spark_matrix(fit, "coefficientMatrix")
#'     colnames(coefficients) <- features
#'
#'     hasIntercept <- invoke(fit, "getFitIntercept")
#'     if (hasIntercept) {
#'       intercept <- read_spark_vector(fit, "interceptVector")
#'       coefficients <- cbind(intercept, coefficients)
#'       colnames(coefficients) <- c("(Intercept)", features)
#'     }
#'
#'     coefficients
#'
#'   } else {
#'
#'     coefficients <- read_spark_vector(fit, "coefficients")
#'
#'     hasIntercept <- invoke(fit, "getFitIntercept")
#'     if (hasIntercept) {
#'       intercept <- invoke(fit, "intercept")
#'       coefficients <- c(coefficients, intercept)
#'       names(coefficients) <- c(features, "(Intercept)")
#'     }
#'
#'     coefficients <- intercept_first(coefficients)
#'     coefficients
#'
#'   }
#'
#'   # multinomial models don't yet provide a 'summary' method
#'   # (as of Spark 2.1.0) so certain features will not be enabled
#'   # for those models
#'   areaUnderROC <- NA
#'   roc <- NA
#'   if (invoke(fit, "hasSummary")) {
#'     summary <- invoke(fit, "summary")
#'     areaUnderROC <- invoke(summary, "areaUnderROC")
#'     roc <- sdf_collect(invoke(summary, "roc"))
#'   }
#'
#'   model <- c(
#'     if (isMultinomial)
#'       "multinomial_logistic_regression"
#'     else
#'       "binomial_logistic_regression",
#'     "logistic_regression"
#'   )
#'
#'   ml_model(model, fit,
#'            features = features,
#'            response = response,
#'            intercept = intercept,
#'            weights.column = weights.column,
#'            coefficients = coefficients,
#'            roc = roc,
#'            area.under.roc = areaUnderROC,
#'            data = df,
#'            ml.options = ml.options,
#'            categorical.transformations = categorical.transformations,
#'            model.parameters = as.list(envir))
#' }
#'

#'

#'
