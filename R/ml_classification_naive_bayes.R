#' Spark ML -- Naive-Bayes
#'
#' Naive Bayes Classifiers. It supports Multinomial NB (see \href{https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html}{here}) which can handle finitely supported discrete data. For example, by converting documents into TF-IDF vectors, it can be used for document classification. By making every vector a binary (0/1) data, it can also be used as Bernoulli NB (see \href{http://nlp.stanford.edu/IR-book/html/htmledition/the-bernoulli-model-1.html}{here}). The input feature values must be nonnegative.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-probabilistic-classifier-params
#' @template roxlate-ml-predictor-params
#' @template roxlate-ml-formula-params
#' @param model_type The model type. Supported options: \code{"multinomial"}
#'   and \code{"bernoulli"}. (default = \code{multinomial})
#' @param smoothing The (Laplace) smoothing parameter. Defaults to 1.
#' @param weight_col (Spark 2.1.0+) Weight column name. If this is not set or empty, we treat all instance weights as 1.0.
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' partitions <- iris_tbl %>%
#'   sdf_random_split(training = 0.7, test = 0.3, seed = 1111)
#'
#' iris_training <- partitions$training
#' iris_test <- partitions$test
#'
#' nb_model <- iris_training %>%
#'   ml_naive_bayes(Species ~ .)
#'
#' pred <- ml_predict(nb_model, iris_test)
#'
#' ml_multiclass_classification_evaluator(pred)
#' }
#'
#' @export
ml_naive_bayes <- function(x, formula = NULL, model_type = "multinomial",
                           smoothing = 1, thresholds = NULL, weight_col = NULL,
                           features_col = "features", label_col = "label",
                           prediction_col = "prediction", probability_col = "probability",
                           raw_prediction_col = "rawPrediction",
                           uid = random_string("naive_bayes_"), ...) {
  check_dots_used()
  UseMethod("ml_naive_bayes")
}

#' @export
ml_naive_bayes.spark_connection <- function(x, formula = NULL, model_type = "multinomial",
                                            smoothing = 1, thresholds = NULL, weight_col = NULL,
                                            features_col = "features", label_col = "label",
                                            prediction_col = "prediction", probability_col = "probability",
                                            raw_prediction_col = "rawPrediction",
                                            uid = random_string("naive_bayes_"), ...) {
  .args <- list(
    model_type = model_type,
    smoothing = smoothing,
    thresholds = thresholds,
    weight_col = weight_col,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    probability_col = probability_col,
    raw_prediction_col = raw_prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_naive_bayes()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.classification.NaiveBayes", uid,
    features_col = .args[["features_col"]], label_col = .args[["label_col"]],
    prediction_col = .args[["prediction_col"]],
    probability_col = .args[["probability_col"]],
    raw_prediction_col = .args[["raw_prediction_col"]]
  ) %>%
    invoke("setSmoothing", .args[["smoothing"]]) %>%
    invoke("setModelType", .args[["model_type"]]) %>%
    jobj_set_param("setThresholds", .args[["thresholds"]]) %>%
    jobj_set_param("setWeightCol", .args[["weight_col"]], "2.1.0")

  new_ml_naive_bayes(jobj)
}

#' @export
ml_naive_bayes.ml_pipeline <- function(x, formula = NULL, model_type = "multinomial",
                                       smoothing = 1, thresholds = NULL, weight_col = NULL,
                                       features_col = "features", label_col = "label",
                                       prediction_col = "prediction", probability_col = "probability",
                                       raw_prediction_col = "rawPrediction",
                                       uid = random_string("naive_bayes_"), ...) {
  stage <- ml_naive_bayes.spark_connection(
    x = spark_connection(x),
    formula = formula,
    model_type = model_type,
    smoothing = smoothing,
    thresholds = thresholds,
    weight_col = weight_col,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    probability_col = probability_col,
    raw_prediction_col = raw_prediction_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_naive_bayes.tbl_spark <- function(x, formula = NULL, model_type = "multinomial",
                                     smoothing = 1, thresholds = NULL, weight_col = NULL,
                                     features_col = "features", label_col = "label",
                                     prediction_col = "prediction", probability_col = "probability",
                                     raw_prediction_col = "rawPrediction",
                                     uid = random_string("naive_bayes_"), response = NULL,
                                     features = NULL, predicted_label_col = "predicted_label", ...) {
  formula <- ml_standardize_formula(formula, response, features)

  stage <- ml_naive_bayes.spark_connection(
    x = spark_connection(x),
    formula = NULL,
    model_type = model_type,
    smoothing = smoothing,
    thresholds = thresholds,
    weight_col = weight_col,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    probability_col = probability_col,
    raw_prediction_col = raw_prediction_col,
    uid = uid,
    ...
  )

  if (is.null(formula)) {
    stage %>%
      ml_fit(x)
  } else {
    ml_construct_model_supervised(
      new_ml_model_naive_bayes,
      predictor = stage,
      formula = formula,
      dataset = x,
      features_col = features_col,
      label_col = label_col,
      predicted_label_col = predicted_label_col
    )
  }
}

# Validator
validator_ml_naive_bayes <- function(.args) {
  .args[["thresholds"]] <- cast_double_list(.args[["thresholds"]], allow_null = TRUE)
  .args[["smoothing"]] <- cast_scalar_double(.args[["smoothing"]])
  .args[["weight_col"]] <- cast_nullable_string(.args[["weight_col"]])
  .args[["model_type"]] <- cast_choice(.args[["model_type"]], c("multinomial", "bernoulli"))
  .args
}

new_ml_naive_bayes <- function(jobj) {
  new_ml_probabilistic_classifier(jobj, class = "ml_naive_bayes")
}

new_ml_naive_bayes_model <- function(jobj) {
  new_ml_probabilistic_classification_model(
    jobj,
    pi = read_spark_vector(jobj, "pi"),
    theta = read_spark_matrix(jobj, "theta"),
    class = "ml_naive_bayes_model"
  )
}
