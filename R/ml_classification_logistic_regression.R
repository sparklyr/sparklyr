#' Spark ML -- Logistic Regression
#'
#' Perform classification using logistic regression.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-linear-regression-params
#' @template roxlate-ml-predictor-params
#' @template roxlate-ml-probabilistic-classifier-params
#' @param family (Spark 2.1.0+) Param for the name of family which is a
#' description of the label distribution to be used in the model. Supported
#' options: "auto", "binomial", and "multinomial."
#' @template roxlate-ml-elastic-net-param
#' @param threshold in binary classification prediction, in range [0, 1].
#' @template roxlate-ml-aggregation-depth
#' @param lower_bounds_on_coefficients (Spark 2.2.0+) Lower bounds on
#' coefficients if fitting under bound constrained optimization.
#' The bound matrix must be compatible with the shape (1, number of features)
#' for binomial regression, or (number of classes, number of features) for
#' multinomial regression.
#' @param lower_bounds_on_intercepts (Spark 2.2.0+) Lower bounds on intercepts
#' if fitting under bound constrained optimization.
#' The bounds vector size must be equal with 1 for binomial regression, or the
#' number of classes for multinomial regression.
#' @param upper_bounds_on_coefficients (Spark 2.2.0+) Upper bounds on
#' coefficients if fitting under bound constrained optimization.
#' The bound matrix must be compatible with the shape (1, number of features)
#' for binomial regression, or (number of classes, number of features) for
#' multinomial regression.
#' @param upper_bounds_on_intercepts (Spark 2.2.0+) Upper bounds on intercepts
#' if fitting under bound constrained optimization. The bounds vector size must
#' be equal with 1 for binomial regression, or the number of classes for
#' multinomial regression.
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' mtcars_tbl <- sdf_copy_to(sc, mtcars, name = "mtcars_tbl", overwrite = TRUE)
#'
#' partitions <- mtcars_tbl %>%
#'   sdf_random_split(training = 0.7, test = 0.3, seed = 1111)
#'
#' mtcars_training <- partitions$training
#' mtcars_test <- partitions$test
#'
#' lr_model <- mtcars_training %>%
#'   ml_logistic_regression(am ~ gear + carb)
#'
#' pred <- ml_predict(lr_model, mtcars_test)
#'
#' ml_binary_classification_evaluator(pred)
#' }
#'
#' @export
ml_logistic_regression <- function(
    x, formula = NULL, fit_intercept = TRUE,
    elastic_net_param = 0, reg_param = 0, max_iter = 100,
    threshold = 0.5, thresholds = NULL, tol = 1e-06,
    weight_col = NULL, aggregation_depth = 2,
    lower_bounds_on_coefficients = NULL, lower_bounds_on_intercepts = NULL,
    upper_bounds_on_coefficients = NULL, upper_bounds_on_intercepts = NULL,
    features_col = "features", label_col = "label", family = "auto",
    prediction_col = "prediction", probability_col = "probability",
    raw_prediction_col = "rawPrediction",
    uid = random_string("logistic_regression_"), ...) {
  check_dots_used()
  UseMethod("ml_logistic_regression")
}

ml_logistic_regression_impl <- function(
    x, formula = NULL, fit_intercept = TRUE,
    elastic_net_param = 0, reg_param = 0, max_iter = 100,
    threshold = 0.5, thresholds = NULL, tol = 1e-06,
    weight_col = NULL, aggregation_depth = 2,
    lower_bounds_on_coefficients = NULL, lower_bounds_on_intercepts = NULL,
    upper_bounds_on_coefficients = NULL, upper_bounds_on_intercepts = NULL,
    features_col = "features", label_col = "label", family = "auto",
    prediction_col = "prediction", probability_col = "probability",
    raw_prediction_col = "rawPrediction",
    response = NULL, features = NULL,
    predicted_label_col = "predicted_label",
    uid = random_string("logistic_regression_"), ...) {

  if (!is.null(formula) && !inherits(x, "tbl_spark")) {
    stop("`formula` may only be specified when `x` is a `tbl_spark`.")
  }

  sc <- spark_connection(x)
  stage <- process_steps(
    x = sc,
    uid = uid,
    spark_class = "org.apache.spark.ml.classification.LogisticRegression",
    invoke_steps = list(
      features_col = cast_string(features_col),
      label_col = cast_string(label_col),
      prediction_col = cast_string(prediction_col),
      probability_col = cast_string(probability_col),
      raw_prediction_col = cast_string(raw_prediction_col),
      weight_col = cast_nullable_string(weight_col),
      tol = cast_scalar_double(tol),
      elastic_net_param = cast_scalar_double(elastic_net_param),
      reg_param = cast_scalar_double(reg_param),
      max_iter = cast_scalar_integer(max_iter),
      family = cast_choice(family, c("auto", "binomial", "multinomial")),
      fit_intercept = cast_scalar_logical(fit_intercept),
      threshold = cast_scalar_double(threshold),
      thresholds = cast_double_list(thresholds, allow_null = TRUE),
      aggregation_depth = cast_scalar_integer(aggregation_depth),
      lower_bounds_on_coefficients = spark_dense_matrix(sc, lower_bounds_on_coefficients),
      upper_bounds_on_coefficients = spark_dense_matrix(sc, upper_bounds_on_coefficients),
      lower_bounds_on_intercepts = spark_dense_vector(sc, lower_bounds_on_intercepts),
      upper_bounds_on_intercepts = spark_dense_vector(sc, upper_bounds_on_intercepts)
      )) %>%
    new_ml_probabilistic_classifier(class = "ml_logistic_regression")

  ret <- NULL
  if(inherits(x, "spark_connection")) {
    ret <- stage
  }
  if(inherits(x, "tbl_spark") && is.null(ret)) {
    formula <- ml_standardize_formula(formula, response, features)
    if (is.null(formula)) {
      ret <- ml_fit(stage, x)
    } else {
      ret <- ml_construct_model_supervised(
        new_ml_model_logistic_regression,
        predictor = stage,
        formula = formula,
        dataset = x,
        features_col = features_col,
        label_col = label_col,
        predicted_label_col = predicted_label_col
      )
    }
  }
  if(inherits(x, "ml_pipeline") && is.null(ret)) {
    ret <- ml_add_stage(x, stage)
  }
  if(is.null(ret)) stop("Object type not recognized")
  ret
}

#' @export
ml_logistic_regression.spark_connection <- ml_logistic_regression_impl

#' @export
ml_logistic_regression.ml_pipeline <- ml_logistic_regression_impl

#' @export
ml_logistic_regression.tbl_spark <- ml_logistic_regression_impl

new_ml_logistic_regression_model <- function(jobj) {
  is_multinomial <- invoke(jobj, "numClasses") > 2

  summary <- if (invoke(jobj, "hasSummary")) {
    if (!is_multinomial && spark_version(spark_connection(jobj)) >= "2.3.0") {
      new_ml_binary_logistic_regression_training_summary(invoke(jobj, "binarySummary"))
    } else {
      new_ml_logistic_regression_training_summary(invoke(jobj, "summary"))
    }
  }

  new_ml_probabilistic_classification_model(
    jobj,
    coefficients = if (is_multinomial) NULL else read_spark_vector(jobj, "coefficients"),
    coefficient_matrix = possibly_null(~ read_spark_matrix(jobj, "coefficientMatrix"))(),
    intercept = if (is_multinomial) NULL else invoke(jobj, "intercept"),
    intercept_vector = possibly_null(~ read_spark_vector(jobj, "interceptVector"))(),
    threshold = if (ml_is_set(jobj, "threshold")) invoke(jobj, "getThreshold") else NULL,
    summary = summary,
    class = "ml_logistic_regression_model"
  )
}

new_ml_logistic_regression_summary <- function(jobj, ..., class = character()) {
  s <- new_ml_summary(
    jobj,
    features_col = function() invoke(jobj, "featuresCol"),
    label_col = function() invoke(jobj, "labelCol"),
    predictions = function() {
      invoke(jobj, "predictions") %>%
        sdf_register()
    },
    probability_col = function() invoke(jobj, "probabilityCol"),
    ...,
    class = c(class, "ml_logistic_regression_summary")
  )

  if (spark_version(spark_connection(jobj)) >= "2.3.0") {
    s$prediction_col <- function() invoke(jobj, "predictionCol")
    s$accuracy <- function() invoke(jobj, "accuracy")
    s$f_measure_by_label <- function(beta = NULL) {
      beta <- cast_nullable_scalar_double(beta)
      if (is.null(beta)) invoke(jobj, "fMeasureByLabel") else invoke(jobj, "fMeasureByLabel", beta)
    }
    s$false_positive_rate_by_label <- function() invoke(jobj, "falsePositiveRateByLabel")
    s$labels <- function() invoke(jobj, "labels")
    s$precision_by_label <- function() invoke(jobj, "precisionByLabel")
    s$recall_by_label <- function() invoke(jobj, "recallByLabel")
    s$true_positive_rate_by_label <- function() invoke(jobj, "truePositiveRateByLabel")
    s$weighted_f_measure <- function(beta = NULL) {
      beta <- cast_nullable_scalar_double(beta)
      if (is.null(beta)) invoke(jobj, "weightedFMeasure") else invoke(jobj, "weightedFMeasure", beta)
    }
    s$weighted_false_positive_rate <- function() invoke(jobj, "weightedFalsePositiveRate")
    s$weighted_precision <- function() invoke(jobj, "weightedPrecision")
    s$weighted_recall <- function() invoke(jobj, "weightedRecall")
    s$weighted_true_positive_rate <- function() invoke(jobj, "weightedTruePositiveRate")
  }

  s
}

new_ml_logistic_regression_training_summary <- function(jobj) {
  new_ml_logistic_regression_summary(
    jobj,
    objective_history = function() invoke(jobj, "objectiveHistory"),
    total_iterations = function() invoke(jobj, "totalIterations"),
    class = "ml_logistic_regression_training_summary"
  )
}

new_ml_binary_logistic_regression_summary <- function(jobj, ..., class = character()) {
  new_ml_logistic_regression_summary(
    jobj,
    area_under_roc = function() invoke(jobj, "areaUnderROC"),
    f_measure_by_threshold = function() {
      invoke(jobj, "fMeasureByThreshold") %>%
        sdf_register()
    },
    pr = function() {
      invoke(jobj, "pr") %>%
        sdf_register()
    },
    precision_by_threshold = function() {
      invoke(jobj, "precisionByThreshold") %>%
        sdf_register()
    },
    recall_by_threshold = function() {
      invoke(jobj, "recallByThreshold") %>%
        sdf_register()
    },
    roc = function() {
      invoke(jobj, "roc") %>%
        sdf_register()
    },
    class = c(class, "ml_binary_logistic_regression_summary")
  )
}

new_ml_binary_logistic_regression_training_summary <- function(jobj) {
  new_ml_binary_logistic_regression_summary(
    jobj,
    objective_history = function() invoke(jobj, "objectiveHistory"),
    total_iterations = function() invoke(jobj, "totalIterations"),
    class = "ml_binary_logistic_regression_training_summary"
  )
}
